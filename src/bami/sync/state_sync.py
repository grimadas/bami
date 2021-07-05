from abc import ABCMeta
import random
from typing import Iterable, Optional, Set, Tuple

from bami.backbone.bloom_filter import (
    BloomFilter,
    BloomFiltersManager,
    bytes_to_bitarray,
    iterate_bloom_filter,
)
from bami.backbone.community_routines import CommunityRoutines, MessageStateMachine
from bami.backbone.exceptions import DatabaseDesynchronizedException
from bami.backbone.utils import decode_raw, encode_raw
from bami.conflicts.caches import PeerSyncCache
from bami.conflicts.peer_management import PeerManagement
from bami.sync.data_models import (
    InconsistentStatePayload,
    PeerState,
    PeerStateRequest,
    RawPeerState,
)
from bami.sync.peer_state import (
    estimate_peer_diff,
    is_state_diff_inconsistent,
    is_state_progressive,
)
from ipv8.lazy_community import lazy_wrapper, lazy_wrapper_wd
from ipv8.peer import Peer
import numpy as np


def filter_missing_txs(
    new_peer_state: PeerState,
    tx_candidates: Iterable[int],
    my_bloom_filter: BloomFilter,
) -> Iterable[int]:
    # Check if peer is missing transactions
    p_blm = new_peer_state.opt_bloom_filter
    if not p_blm:
        return set()
    peer_bits = bytes_to_bitarray(p_blm.filter_bits)
    return {
        t
        for t in tx_candidates
        if np.any(peer_bits[list(my_bloom_filter.get_indices(t))] == 0)
    }


class PeerStateSyncMixin(
    MessageStateMachine, PeerManagement, CommunityRoutines, metaclass=ABCMeta
):
    """
    Request and response via exchange of peer_states:
     - Request a state update
     - Response with a last state
     - Request state transactions
     - Request state of other peers
    """

    STATE_SYNC_PREFIX = "state_sync"

    def setup_mixin(self):
        self.blm_filter_manager = BloomFiltersManager(self.my_peer.mid)

    def setup_messages(self) -> None:
        self.add_message_handler(PeerStateRequest, self.received_state_request)
        self.add_message_handler(RawPeerState, self.received_raw_peer_state)
        self.add_message_handler(PeerState, self.received_peer_state)
        self.add_message_handler(
            InconsistentStatePayload, self.received_exposed_status_message
        )

    # ---------- Inconsistencies ------------

    def on_inconsistent_state(
        self,
        signer_id: bytes,
        state_id: bytes,
        state_1: bytes,
        state_2: bytes,
        share_message: bool = True,
    ) -> None:
        """
        React on receiving two inconsistent state updates.
        """
        if share_message:
            self.share_in_community(
                InconsistentStatePayload(encode_raw((state_1, state_2))), state_id
            )
        self.on_exposed_peer(signer_id, state_id, state_1, state_2)

    @lazy_wrapper(InconsistentStatePayload)
    def received_exposed_status_message(
        self, peer: Peer, payload: InconsistentStatePayload
    ) -> None:
        state_1, state_2 = decode_raw(payload.pack)

        signer, new_peer_state = self.parse_raw_packet(state_2, PeerState)
        state_id = new_peer_state.state_id
        signer_id = signer.mid

        # TODO: add payload message verification
        self.on_inconsistent_state(
            signer_id, state_id, state_1, state_2, share_message=False
        )

    # ---------- State comparison and settlement ------------

    def on_settled_transactions(self, tx_ids: Set[int]) -> None:
        pass

    def settle_transactions(
        self,
        signer_id: bytes,
        state_id: bytes,
        state_diff: np.ndarray,
        last_peer_state: Optional[PeerState],
    ) -> Iterable[int]:
        """
        Iterate bloom filter and remove settled transactions (present in bloom filters and in state)
        @param state_diff: Diff between new received state and last state
        @param last_peer_state:
        @return: Unsettled transactions
        """
        p_diff = self.peer_store.get_last_peer_state_diff(signer_id, state_id)
        last_bf = last_peer_state.opt_bloom_filter if last_peer_state else None
        # Iterate bloom filter, updating p_diff
        new_txs, settled_txs = iterate_bloom_filter(
            self.blm_filter_manager, p_diff, state_id, signer_id, last_bf
        )
        self.logger.debug("Settle transactions %s", settled_txs)
        self.logger.debug("Add new transactions %s", new_txs)
        self.on_settled_transactions(settled_txs)
        self.peer_store.store_peer_state_diff(signer_id, state_id, p_diff + state_diff)
        return new_txs

    def on_detecting_missing_txs(
        self, peer: Peer, txs_hashes: Iterable[int], state_id: bytes
    ) -> None:
        """Peer is missing transactions. Send missing txs_hashes and update sync-tick on peer"""
        changed = len(txs_hashes) > 0
        for t_hash in txs_hashes:
            tx_pack = self.persistence.get_transaction_by_hash(t_hash)
            if not tx_pack:
                raise DatabaseDesynchronizedException(
                    "{} not in the db!".format(t_hash)
                )
            self.send_packet(peer.address, tx_pack)

        async def start_request():
            try:
                # Check if we are still missing
                self.request_state_update(
                    peer, state_id, self.settings.max_request_tries
                )
            except Exception as e:
                self.logger.info(
                    "Error encountered during request state (error: %s)", e
                )

        if changed:
            delay = self.settings.min_request_delay + random.random() * (
                self.settings.max_request_delay - self.settings.min_request_delay
            )
            self.request_cache.register_anonymous_task(
                "start-request-" + str(hash(peer)), start_request, delay=delay
            )

    def my_blm_filter(self, state_id: bytes, counter_party_id: bytes) -> BloomFilter:
        return self.blm_filter_manager.bloom_filter(state_id, counter_party_id)

    def on_new_state(self, sender: Peer, signed_peer_state: bytes) -> PeerState:
        """
        Process a signed new state packet received from sender.
        """
        signer, new_peer_state = self.parse_raw_packet(signed_peer_state, PeerState)
        state_id = new_peer_state.state_id
        signer_id = signer.mid
        last_peer_state = self.peer_store.get_last_peer_state(signer_id, state_id)
        state_diff = estimate_peer_diff(new_peer_state, last_peer_state)
        if is_state_diff_inconsistent(state_diff):
            lps = self.peer_store.get_last_signed_peer_state(signer_id, state_id)
            self.on_inconsistent_state(signer.mid, state_id, lps, signed_peer_state)
        else:
            if is_state_progressive(state_diff):
                self.peer_store.store_peer_state(signer_id, new_peer_state)
                self.peer_store.store_signed_peer_state(
                    signer_id, state_id, signed_peer_state
                )
                # Pop pending request if any
                prefix = self.STATE_SYNC_PREFIX
                cache_id = hash(signer)

                if self.request_cache.has(prefix, cache_id):
                    self.request_cache.pop(prefix, cache_id)

            if sender.mid == signer.mid:
                my_blm = self.my_blm_filter(state_id, signer_id)
                self.logger.info(
                    "Received request, sending with my blm: %s", my_blm,
                )
                new_txs = self.settle_transactions(
                    signer_id, state_id, state_diff, last_peer_state
                )
                missing_txs = filter_missing_txs(new_peer_state, new_txs, my_blm)
                self.on_detecting_missing_txs(sender, missing_txs, state_id)
        return new_peer_state

    def missing_peer_update(
        self, peer_id: bytes, state_id: bytes, requested_state: PeerState
    ) -> bool:
        last_state = self.peer_store.get_last_peer_state(peer_id, state_id)
        state_diff = estimate_peer_diff(requested_state, last_state)
        return is_state_progressive(
            state_diff
        )  # requested state contains newer transactions

    def prepare_state_request(
        self, state_id: bytes, requested_peers: Set[bytes], counter_party_id: bytes
    ) -> Tuple[PeerStateRequest, PeerState]:
        # last cell
        ps = self.persistence.get_peer_state(
            state_id, self.my_blm_filter(state_id, counter_party_id)
        )
        signed_state = self.prepare_packet(ps)
        return PeerStateRequest(signed_state, requested_peers), ps

    def request_state_update(
        self, peer: Peer, state_id: bytes, max_tries: int = 5,
    ):
        prefix = self.STATE_SYNC_PREFIX
        if self.request_cache.has(prefix, hash(peer)):
            self.request_cache.pop(prefix, hash(peer))

        psr, ps = self.prepare_state_request(state_id, {peer.mid}, peer.mid)

        # prepare peer state request
        if self.missing_peer_update(peer.mid, state_id, ps):
            self.send_payload(peer, psr, sig=True)
            self.request_cache.add(
                PeerSyncCache(
                    self.request_cache,
                    self,
                    prefix,
                    peer,
                    state_id,
                    max_tries - 1,
                    self.request_state_update,
                )
            )

    @lazy_wrapper(PeerStateRequest)
    def received_state_request(self, peer: Peer, payload: PeerStateRequest) -> None:
        """
        React on state request.
        @param peer: that send the packet request
        @param payload: State request packet
        """
        psr = payload
        peer_state = self.on_new_state(peer, psr.signed_peer_state)
        state_id = peer_state.state_id
        for t in psr.requested_peers:
            if t == self.my_peer.mid:
                # Prepare new peer state
                peer_state = self.persistence.get_peer_state(
                    state_id, self.my_blm_filter(state_id, peer.mid)
                )
                self.send_payload(peer, peer_state, sig=True)
            else:
                stored_t_state = self.peer_store.get_last_peer_state(t, state_id)
                peer_diff = estimate_peer_diff(stored_t_state, peer_state)

                if is_state_progressive(peer_diff, strict=False):
                    signed_t_state = self.peer_store.get_last_signed_peer_state(
                        t, state_id
                    )
                    self.send_payload(peer, RawPeerState(signed_t_state), sig=True)

    @lazy_wrapper_wd(PeerState)
    def received_peer_state(self, p: Peer, _, data: bytes) -> None:
        self.on_new_state(p, data)

    @lazy_wrapper(RawPeerState)
    def received_raw_peer_state(self, p: Peer, payload: RawPeerState) -> None:
        self.on_new_state(p, payload.value)

    def peer_sync_task(self, state_id: bytes) -> None:
        """Peer sync task. Keep peer in sync for the application """
        ps = self.persistence.get_peer_state(state_id, None)
        peers = self.get_next_peers_for_request(state_id)
        for p in peers:
            if self.missing_peer_update(p.mid, state_id, requested_state=ps):
                self.request_state_update(p, state_id, self.settings.max_request_tries)
