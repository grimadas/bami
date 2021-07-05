from abc import ABC, abstractmethod
from typing import Iterable, Optional, Tuple
from collections import defaultdict

from bami.backbone.utils import decode_raw, encode_raw, take_hash, CellsArray
from bami.sync.peer_state import CellIndexer
from bami.sync.data_models import CellState, PeerState, FilterPack
from bami.backbone.transaction import Transaction

import lmdb
import numpy as np


class BaseBlockStore(ABC):
    """Store interface for tx blobs"""

    @abstractmethod
    def add_transaction(self, tx: Transaction, tx_blob: bytes) -> None:
        pass

    @abstractmethod
    def get_transaction_by_hash(self, tx_hash: int) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_peer_state(self, state_id: bytes, blm_filter: "BloomFilter") -> PeerState:
        pass

    @abstractmethod
    def store_peer_state(self, state_id: bytes, new_peer_state: PeerState) -> None:
        pass

    @abstractmethod
    def update_peer_state(self, state_id: bytes, tx_hash: int) -> None:
        pass

    @abstractmethod
    def get_cells_array(self, state_id: bytes) -> CellsArray:
        pass

    @abstractmethod
    def get_cells_state(self, state_id: bytes) -> CellState:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


def create_new_cell_state():
    ca = CellIndexer.create_cell_array()
    return CellIndexer.to_packable_cells(ca)


def update_cell_state(tx_hash: int, cell_state: CellState):
    pass


class LMDBLockStore(BaseBlockStore):
    """BlockStore implementation based on LMBD"""

    INT_BYTES = 8
    BYTE_ORDER = "big"

    def __init__(self, block_dir: str) -> None:
        # Change the directory
        self.env = lmdb.open(block_dir, subdir=True, max_dbs=5, map_async=True)
        self.transactions = self.env.open_db(key=b"transactions")
        self.my_states = self.env.open_db(key=b"my_states")

        self.cells = defaultdict(lambda: CellIndexer.create_cell_array())
        self.joined_hashes = defaultdict(lambda: [])
        self.join_hash = {}
        # add sub dbs if required

    def add_transaction(self, tx: Transaction, tx_blob: bytes) -> None:
        tx_hash = hash(tx)
        with self.env.begin(write=True) as txn:
            txn.put(
                tx_hash.to_bytes(
                    LMDBLockStore.INT_BYTES, LMDBLockStore.BYTE_ORDER, signed=True
                ),
                tx_blob,
                db=self.transactions,
            )

    def get_cells_array(self, state_id: bytes) -> Tuple[int]:
        return self.cells[state_id]

    def get_cells_state(self, state_id: bytes) -> CellState:
        return CellIndexer.to_packable_cells(np.array(self.cells[state_id]))

    def get_transaction_by_hash(self, tx_hash: int) -> Optional[bytes]:
        with self.env.begin() as txn:
            val = txn.get(
                tx_hash.to_bytes(
                    LMDBLockStore.INT_BYTES, LMDBLockStore.BYTE_ORDER, signed=True
                ),
                db=self.transactions,
            )
        return val

    def get_peer_state(self, state_id: bytes, blm_filter: "BloomFilter") -> PeerState:
        return PeerState(
            self.get_cells_state(state_id),
            self.join_hash.get(state_id, b'0'),
            state_id,
            blm_filter.to_filter_pack() if blm_filter else FilterPack(b"", 0),
        )

    def store_peer_state(self, state_id: bytes, new_peer_state: PeerState) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(
                state_id, new_peer_state.to_msgpack(), db=self.my_states,
            )

    def update_peer_state(self, state_id: bytes, tx_hash: int) -> None:
        # 1. Update cells
        self.cells[state_id][CellIndexer.cell_id(tx_hash)] += 1
        # 2. Update join hash
        self.joined_hashes[state_id].append(tx_hash)
        self.joined_hashes[state_id] = sorted(self.joined_hashes[state_id])
        self.join_hash[state_id] = take_hash(self.joined_hashes[state_id])

    def close(self) -> None:
        self.env.close()
