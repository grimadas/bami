from abc import ABCMeta
from typing import Any, Dict, Iterable, Optional, Type, Union

from bami.backbone.community import BamiCommunity
from bami.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.backbone.discovery import SubCommunityDiscoveryStrategy
from bami.backbone.settings import BamiSettings
from bami.conflicts.conflict_set import (
    BaseConflictSet,
    BaseConflictSetFactory,
    IPv8ConflictSetFactory,
    LightConflictSetFactory,
    SubCommunityRoutines,
)
from bami.datastore.database import BaseDB
from ipv8.community import Community
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.keys import Key
from ipv8.messaging.payload import Payload
from ipv8.peer import Address, Peer
from ipv8.requestcache import RequestCache

from tests.mocking.mock_db import MockDBManager


class FakeRoutines(CommunityRoutines):
    def send_packet(self, address: Address, packet: bytes) -> None:
        pass

    def prepare_packet(self, payload: Payload, sig: bool = True) -> bytes:
        pass

    def share_in_community(
        self,
        packet: Union[bytes, Payload],
        subcom_id: bytes = None,
        fanout: int = None,
        seed: int = None,
    ) -> None:
        pass

    @property
    def request_cache(self) -> RequestCache:
        pass

    @property
    def ipv8(self) -> Optional[Any]:
        pass

    @property
    def settings(self) -> Any:
        return BamiSettings()

    def __init__(self):
        self.crypto = default_eccrypto
        self.key = self.crypto.generate_key(u"medium")

    @property
    def my_peer_key(self) -> Key:
        return self.key

    @property
    def my_pub_key(self) -> bytes:
        return self.key.pub().key_to_bin()

    def send_payload(self, peer: Peer, payload: Any) -> None:
        pass

    @property
    def persistence(self) -> BaseDB:
        return MockDBManager()


class MockSubCommuntiy(BaseConflictSet):
    async def unload(self):
        pass

    def add_peer(self, peer: Peer):
        pass

    @property
    def cs_id(self) -> bytes:
        pass

    def get_known_peers(self) -> Iterable[Peer]:
        pass


class MockSubCommunityDiscoveryStrategy(SubCommunityDiscoveryStrategy):
    def discover(
        self,
        subcom: BaseConflictSet,
        target_peers: int = 20,
        discovery_params: Dict[str, Any] = None,
    ) -> None:
        pass


class MockConflictSetFactory(BaseConflictSetFactory):
    def create_conflict_set(self, *args, **kwargs) -> BaseConflictSet:
        return MockSubCommuntiy()


class MockSubCommunityRoutines(SubCommunityRoutines):
    def discovered_peers_by_conflict_set(self, subcom_id) -> Iterable[Peer]:
        pass

    def get_conflict_set(self, sub_com: bytes) -> Optional[BaseConflictSet]:
        pass

    @property
    def my_conflict_sets(self) -> Iterable[bytes]:
        pass

    def add_conflict_set(self, sub_com: bytes, subcom_obj: BaseConflictSet) -> None:
        pass

    def notify_peers_on_new_conflict_set(self) -> None:
        pass

    def on_join_cs(self, cs_id: bytes) -> None:
        pass

    @property
    def cs_factory(
        self,
    ) -> Union[BaseConflictSetFactory, Type[BaseConflictSetFactory]]:
        return MockConflictSetFactory()


class MockSettings(object):
    @property
    def frontier_gossip_collect_time(self):
        return 0.2

    @property
    def frontier_gossip_fanout(self):
        return 5


class MockedCommunity(Community, CommunityRoutines):
    def send_packet(self, address: Address, packet: bytes) -> None:
        return BamiCommunity.send_packet(self, address, packet)

    def share_in_community(
        self,
        packet: Union[bytes, Payload],
        subcom_id: bytes = None,
        fanout: int = None,
        seed: int = None,
    ) -> None:
        return BamiCommunity.share_in_community(self, packet, subcom_id, fanout, seed)

    community_id = Peer(default_eccrypto.generate_key(u"very-low")).mid

    def __init__(self, *args, **kwargs):
        if kwargs.get("work_dirs"):
            self.work_dirs = kwargs.pop("work_dirs")
        super().__init__(*args, **kwargs)
        self._req = RequestCache()

        for base in self.__class__.__bases__:
            if issubclass(base, MessageStateMachine):
                base.setup_messages(self)

    @property
    def persistence(self) -> BaseDB:
        return MockDBManager()

    @property
    def settings(self) -> Any:
        return MockSettings()

    def send_payload(self, *args, **kwargs) -> None:
        self.ez_send(*args, **kwargs)

    def prepare_packet(self, payload: Payload, sig: bool = True) -> bytes:
        return self.ezr_pack(payload.msg_id, payload, sig=sig)

    @property
    def request_cache(self) -> RequestCache:
        return self._req

    async def unload(self):
        await self._req.shutdown()
        return await super().unload()


class FakeBackCommunity(BamiCommunity, metaclass=ABCMeta):
    community_id = b"\x00" * 20


class FakeIPv8BackCommunity(BamiCommunity, IPv8ConflictSetFactory):
    pass


class FakeLightBackCommunity(LightConflictSetFactory, BamiCommunity):
    pass
