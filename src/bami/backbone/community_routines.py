from abc import ABC, abstractmethod
from typing import Callable, Optional, Tuple, Type, Union

from bami.backbone.settings import BamiSettings
from bami.datastore.database import BaseDB
from ipv8.keyvault.keys import Key
from ipv8.lazy_community import PacketDecodingError
from ipv8.messaging.payload import Payload
from ipv8.messaging.payload_headers import BinMemberAuthenticationPayload
from ipv8.messaging.serialization import Serializable
from ipv8.peer import Address, Peer
from ipv8.requestcache import RequestCache


class CommunityRoutines(ABC):
    @property
    def my_peer_key(self) -> Key:
        return self.my_peer.key

    @property
    def my_pub_key_bin(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    @property
    @abstractmethod
    def request_cache(self) -> RequestCache:
        pass

    @abstractmethod
    def send_payload(self, peer: Peer, payload: Payload, sig: bool = True) -> None:
        """Send payload_cls payload_cls to the peer"""
        pass

    @abstractmethod
    def send_packet(self, address: Address, packet: bytes) -> None:
        """Send payload_cls payload_cls to the peer"""
        pass

    @abstractmethod
    def prepare_packet(self, payload: Payload, sig: bool = True) -> bytes:
        """Pack and sign Payload"""
        pass

    @abstractmethod
    def share_in_community(
        self,
        packet: Union[bytes, Payload],
        subcom_id: bytes = None,
        fanout: int = None,
        seed: int = None,
    ) -> None:
        pass

    def parse_raw_packet(
        self,
        packet: bytes,
        payload_class: Type[Serializable],
        source_address: Optional[Address] = None,
    ) -> Tuple[Peer, Serializable]:
        """Unpack and verify the payload_cls"""

        auth, _ = self.serializer.unpack_serializable(
            BinMemberAuthenticationPayload, packet, offset=23
        )
        signature_valid, remainder = self._verify_signature(auth, packet)
        unpacked_payload, _ = self.serializer.unpack_serializable(
            payload_class, remainder, offset=23
        )
        # ASSERT
        if not signature_valid:
            raise PacketDecodingError(
                "Incoming packet %s has an invalid signature"
                % str(payload_class.__name__)
            )
        # PRODUCE
        peer = self.network.verified_by_public_key_bin.get(auth.public_key_bin) or Peer(
            auth.public_key_bin, source_address
        )
        return peer, unpacked_payload

    @property
    @abstractmethod
    def persistence(self) -> BaseDB:
        pass

    @property
    @abstractmethod
    def settings(self) -> BamiSettings:
        pass


class MessageStateMachine(ABC):
    @abstractmethod
    def add_message_handler(
        self, payload_class: Type[Payload], handler: Callable
    ) -> None:
        pass

    @abstractmethod
    def setup_messages(self) -> None:
        pass
