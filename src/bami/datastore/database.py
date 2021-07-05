"""
This file contains everything related to persistence for TrustChain.
"""
from abc import abstractmethod
from enum import Enum
import logging
from typing import Dict

from bami.backbone.transaction import Transaction
from bami.backbone.utils import Notifier
from bami.datastore.block_store import BaseBlockStore, LMDBLockStore
from bami.datastore.causality_engine import CausalEngine
from bami.datastore.chain_store import Chain
from bami.datastore.heap_store import Heap


class BaseDB(BaseBlockStore):
    @abstractmethod
    def has_transaction(self, tx_hash: int) -> bool:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class ProcessingType(Enum):
    CHAIN = 1
    HEAP = 2


class DBManager(LMDBLockStore, BaseDB):
    def __init__(
        self, block_dir: str, default_processing: ProcessingType = ProcessingType.CHAIN
    ) -> None:
        super().__init__(block_dir)
        self.blk_dir = block_dir

        self.default_processing = default_processing
        self.processing_types = {}
        self.causal_engines: Dict[bytes, CausalEngine] = {}

        self.logger = logging.getLogger("📒 DB")
        self.order_tx = Notifier()
        self.causal_indexes = {}

    def set_processing_type(self, state_id: bytes, pt: ProcessingType) -> None:
        self.processing_types[state_id] = pt

    def close(self) -> None:
        super().close()

    def has_transaction(self, tx_hash: int) -> bool:
        return self.get_transaction_by_hash(tx_hash) is not None

    def init_heap_engine(self, state_id: bytes):
        self.causal_engines[state_id] = Heap()

    def init_chain_engine(self, state_id: bytes):
        self.causal_engines[state_id] = Chain()

    def get_engine(self, state_id: bytes) -> CausalEngine:
        return self.causal_engines[state_id]

    def add_transaction(self, tx: Transaction, tx_blob: bytes) -> None:
        # add transaction to block store
        super().add_transaction(tx, tx_blob)

        # Update peer state
        s_id = tx.state_id
        self.update_peer_state(s_id, hash(tx))
        if s_id not in self.causal_engines:
            if self.default_processing == ProcessingType.CHAIN:
                self.init_chain_engine(s_id)
            else:
                self.init_heap_engine(s_id)
            self.logger.debug(
                "%s on %s created", self.causal_engines[s_id], s_id
            )
        engine = self.causal_engines[s_id]
        self.order_tx.notify(s_id, engine.add_tx(tx))
