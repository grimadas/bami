from abc import ABCMeta

from bami.backbone.bloom_filter import filter_id, TxIndexer
from bami.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.backbone.transaction import Transaction
from bami.conflicts.peer_management import PeerManagement
from ipv8.lazy_community import lazy_wrapper_wd
from ipv8.peer import Peer


class TransactionSyncMixin(
    MessageStateMachine, PeerManagement, CommunityRoutines, metaclass=ABCMeta
):
    def setup_messages(self) -> None:
        self.add_message_handler(Transaction, self.received_transaction)

    @lazy_wrapper_wd(Transaction)
    def received_transaction(self, peer: Peer, tx: Transaction, data: bytes):
        self.validate_persist_transaction(data, tx, peer)

    def process_transaction_out_of_order(self, tx: Transaction, peer: Peer) -> None:
        """
        Process a received half tx immediately when received. Does not guarantee order on the tx.
        """
        pass

    def process_transaction_in_order(self, tx: Transaction) -> None:
        """
        Process a tx that we have received after all causal creator_state were downloaded.

        Args:
            tx: The received tx.

        """
        pass

    def update_tx_indexes(self, tx: Transaction) -> None:
        s_id = tx.state_id
        p_ids = self.peer_store.get_peers_by_state_id(s_id)
        p_ids = p_ids if p_ids else []
        for p_id in p_ids:
            f_id = filter_id(s_id, p_id)
            TxIndexer.add_new_tx(f_id, hash(tx))

    def validate_persist_transaction(
        self, signed_packet: bytes, tx: Transaction, peer: Peer = None
    ) -> bool:
        self.logger.info("%s received from 🖥 %s", tx, peer)
        if not self.persistence.has_transaction(hash(tx)):
            self.process_transaction_out_of_order(tx, peer)
            self.persistence.add_transaction(tx, signed_packet)
            # Update tx indexes
            self.update_tx_indexes(tx)
        else:
            self.logger.warning("Transaction %s already present!", tx)

    def sign_transaction(self, tx: Transaction) -> bytes:
        return self.prepare_packet(tx, sig=True)

    def create_and_store_tx(self, tx: Transaction):
        tx_packet = self.sign_transaction(tx)
        self.validate_persist_transaction(tx_packet, tx)
        return tx_packet
