from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Iterable

from bami.backbone.community_routines import CommunityRoutines
from bami.backbone.utils import Notifier
from bami.datastore.peer_store import BasePeerStore
from ipv8.peer import Peer


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
    def from_bytes(cls, enum_obj: bytes) -> "PeerStatus":
        return PeerStatus(int.from_bytes(enum_obj, byteorder="big", signed=False))


class PeerManagement(Notifier, CommunityRoutines, metaclass=ABCMeta):
    """PeerManagement in the conflict set"""

    @property
    @abstractmethod
    def peer_store(self) -> BasePeerStore:
        pass

    @abstractmethod
    def get_next_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass

    @abstractmethod
    def get_prev_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass

    def on_exposed_peer(
        self, signer_id: bytes, state_id: bytes, state_1: bytes, state_2: bytes
    ):
        # Put in the database
        self.peer_store.store_inconsistent_state(signer_id, state_1, state_2)
        self.peer_store.update_peer_status(signer_id, PeerStatus.EXPOSED)
        self.notify(PeerStatus.EXPOSED, signer_id, state_id, state_1, state_2)
