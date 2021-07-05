from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
import logging
from typing import Iterable, Optional

from bami.backbone.utils import encode_raw
from bami.sync.data_models import PeerState
from bami.sync.peer_state import CellIndexer
import lmdb
import numpy as np


class PeerStatus(Enum):
    """
    Possible peer status
    """

    LIVE = 1
    IDLE = 2
    EXPOSED = 3
    SUSPECTED = 4

    def to_bytes(self) -> bytes:
        return int.to_bytes(self.value, 1, byteorder="big", signed=False)

    @classmethod
    def from_bytes(cls, enum_obj: bytes) -> PeerStatus:
        return PeerStatus(int.from_bytes(enum_obj, byteorder="big", signed=False))


class BasePeerStore(ABC):
    """Store interface for tx blobs"""

    @abstractmethod
    def update_peer_status(self, peer_mid: bytes, new_peer_status: PeerStatus) -> None:
        pass

    @abstractmethod
    def get_peer_status(self, peer_mid: bytes) -> Optional[PeerStatus]:
        pass

    @abstractmethod
    def get_peers_by_status(self, status: PeerStatus) -> Iterable[bytes]:
        pass

    @abstractmethod
    def store_peer_state(self, peer_mid: bytes, peer_state: PeerState) -> None:
        pass

    @abstractmethod
    def store_inconsistent_state(
        self, signer_peer: bytes, state_1: bytes, state_2: bytes
    ) -> None:
        pass

    @abstractmethod
    def get_last_peer_state(self, peer_mid: bytes, state_id: bytes) -> PeerState:
        pass

    @abstractmethod
    def get_last_peer_state_diff(self, peer_mid: bytes, state_id: bytes) -> np.ndarray:
        pass

    @abstractmethod
    def store_peer_state_diff(
        self, peer_mid: bytes, state_id: bytes, state_diff: np.ndarray
    ) -> None:
        pass

    @abstractmethod
    def store_signed_peer_state(
        self, peer_mid: bytes, state_id: bytes, peer_state: bytes
    ) -> None:
        pass

    @abstractmethod
    def get_last_signed_peer_state(self, peer_mid: bytes, state_id: bytes) -> bytes:
        pass

    @abstractmethod
    def get_peers_by_state_id(self, state_id: bytes) -> Iterable[bytes]:
        pass

    @abstractmethod
    def add_peer_state_id(self, state_id: bytes, peer_id: bytes) -> None:
        pass


class PeerStore(BasePeerStore):
    def __init__(self, block_dir: str) -> None:
        # Change the directory
        self.env = lmdb.open(block_dir, subdir=True, max_dbs=5, map_async=True)
        self.state = self.env.open_db(key=b"state")
        self.raw_state = self.env.open_db(key=b"r_state")
        self.status = self.env.open_db(key=b"status")
        self.inconsistencies = self.env.open_db(key=b"exposed")

        # add sub dbs if required
        self.i_status = defaultdict(lambda: set())
        self.peer_infos = defaultdict(lambda: set())
        self.i_state = defaultdict(lambda: set())  # map peers to known state_id
        self.peer_state_diff = {}

        # Local bloom filters: state -> peer_id -> cell: BloomFilter
        self.filters = defaultdict(lambda: defaultdict(lambda: {}))

        self.logger = logging.getLogger('PeerStore')

    def get_peers_by_state_id(self, state_id: bytes) -> Iterable[bytes]:
        return self.i_state[state_id]

    def add_peer_state_id(self, state_id: bytes, peer_id: bytes) -> None:
        self.i_state[state_id].add(peer_id)

    def store_inconsistent_state(
        self, signer_peer: bytes, state_1: bytes, state_2: bytes
    ) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(
                signer_peer, encode_raw((state_1, state_2)), db=self.inconsistencies
            )

    def get_last_peer_state_diff(self, peer_mid: bytes, state_id: bytes) -> np.ndarray:
        return self.peer_state_diff.get(
            peer_mid + state_id, CellIndexer.create_cell_array()
        )

    def store_peer_state_diff(
        self, peer_mid: bytes, state_id: bytes, state_diff: np.ndarray
    ) -> None:
        self.peer_state_diff[peer_mid + state_id] = state_diff

    def update_peer_status(self, peer_mid: bytes, new_peer_status: PeerStatus) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(peer_mid, new_peer_status.to_bytes(), db=self.status)
            self.i_status[new_peer_status.to_bytes()].add(peer_mid)

    def get_peer_status(self, peer_mid: bytes) -> Optional[PeerStatus]:
        with self.env.begin() as txn:
            txn.replace()
            val = txn.get(peer_mid, db=self.status)
        return val if val else PeerStatus.from_bytes(val)

    def get_peers_by_status(self, status: PeerStatus) -> Iterable[bytes]:
        return self.i_status[status.to_bytes()]

    def get_last_peer_state(
        self, peer_mid: bytes, state_id: bytes
    ) -> Optional[PeerState]:
        # Fetch last cells from peers
        s_id = peer_mid + state_id
        with self.env.begin() as txn:
            cells = txn.get(s_id, db=self.state)
        if not cells:
            return None
        return PeerState.from_bytes(cells)

    def store_peer_state(self, peer_mid: bytes, peer_state: PeerState) -> None:
        state_id = peer_state.state_id
        s_id = peer_mid + state_id
        self.add_peer_state_id(state_id, peer_mid)

        with self.env.begin(write=True) as txn:
            txn.put(s_id, peer_state.to_bytes(), db=self.state)

    def store_signed_peer_state(
        self, peer_mid: bytes, state_id: bytes, signed_peer_state: bytes
    ) -> None:
        s_id = peer_mid + state_id
        with self.env.begin(write=True) as txn:
            txn.put(s_id, signed_peer_state, db=self.raw_state)

    def get_last_signed_peer_state(
        self, peer_mid: bytes, state_id: bytes
    ) -> Optional[bytes]:
        s_id = peer_mid + state_id
        with self.env.begin() as txn:
            raw_state = txn.get(s_id, db=self.raw_state)
        return raw_state
