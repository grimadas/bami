from typing import Iterator, Optional, Iterable, Set, Tuple

from bami.backbone.transaction import Transaction
from bami.datastore.block_store import BaseBlockStore
from bami.datastore.chain_store import BaseChain, BaseChainFactory
from bami.datastore.database import BaseDB
from bami.backbone.utils import CellsArray, Dot, Links, ShortKey
from bami.datastore.frontiers import Frontier, FrontierDiff
from bami.datastore.peer_store import BasePeerStore, PeerStatus
from bami.sync.data_models import CellState, PeerState
import numpy as np


class MockBlockStore(BaseBlockStore):
    def get_cells_state(self, state_id: bytes) -> CellState:
        pass

    def add_transaction(self, tx: Transaction, tx_blob: bytes) -> None:
        pass

    def get_transaction_by_hash(self, tx_hash: int) -> Optional[bytes]:
        pass

    def get_peer_state(self, state_id: bytes) -> PeerState:
        pass

    def store_peer_state(self, state_id: bytes, new_peer_state: PeerState) -> None:
        pass

    def update_peer_state(self, state_id: bytes, tx_hash: int) -> None:
        pass

    def get_cells_array(self, state_id: bytes) -> CellsArray:
        pass

    def close(self) -> None:
        pass


class MockDBManager(BaseDB, MockBlockStore):
    def has_transaction(self, tx_hash: int) -> bool:
        pass

    def close(self) -> None:
        pass


class MockPeerStore(BasePeerStore):
    def update_peer_status(self, peer_mid: bytes, new_peer_status: PeerStatus) -> None:
        pass

    def get_peer_status(self, peer_mid: bytes) -> Optional[PeerStatus]:
        pass

    def get_peers_by_status(self, status: PeerStatus) -> Iterable[bytes]:
        pass

    def store_peer_state(self, peer_mid: bytes, peer_state: PeerState) -> None:
        pass

    def store_inconsistent_state(
        self, signer_peer: bytes, state_1: bytes, state_2: bytes
    ) -> None:
        pass

    def get_last_peer_state(self, peer_mid: bytes, state_id: bytes) -> PeerState:
        pass

    def get_last_peer_state_diff(self, peer_mid: bytes, state_id: bytes) -> np.ndarray:
        pass

    def store_peer_state_diff(
        self, peer_mid: bytes, state_id: bytes, state_diff: np.ndarray
    ) -> None:
        pass

    def store_signed_peer_state(
        self, peer_mid: bytes, state_id: bytes, peer_state: bytes
    ) -> None:
        pass

    def get_last_signed_peer_state(self, peer_mid: bytes, state_id: bytes) -> bytes:
        pass

    def get_peers_by_state_id(self, state_id: bytes) -> Iterable[bytes]:
        pass

    def add_peer_state_id(self, state_id: bytes, peer_id: bytes) -> None:
        pass


class MockChain(BaseChain):
    def add_transaction(
        self, block_links: Links, block_seq_num: int, block_hash: bytes
    ) -> Iterable[Dot]:
        pass

    def reconcile(
        self, frontier: Frontier, last_reconcile_point: int = None
    ) -> FrontierDiff:
        pass

    @property
    def frontier(self) -> Frontier:
        pass

    @property
    def consistent_terminal(self) -> Links:
        pass

    @property
    def terminal(self) -> Links:
        pass

    def get_next_links(self, block_dot: Dot) -> Optional[Links]:
        pass

    def get_prev_links(self, block_dot: Dot) -> Optional[Links]:
        pass

    def get_dots_by_seq_num(self, seq_num: int) -> Iterable[Dot]:
        pass

    def get_all_short_hash_by_seq_num(self, seq_num: int) -> Optional[Set[ShortKey]]:
        pass


class MockChainFactory(BaseChainFactory):
    def create_chain(self, **kwargs) -> BaseChain:
        return MockChain()
