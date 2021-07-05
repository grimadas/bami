from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Type, Union

from bami.backbone.community_routines import CommunityRoutines
from bami.backbone.exceptions import UnavailableIPv8Exception
from bami.datastore.peer_store import BasePeerStore
from ipv8.community import Community
from ipv8.peer import Peer


class BaseConflictSet(ABC):
    @property
    @abstractmethod
    def cs_id(self) -> bytes:
        pass

    @abstractmethod
    def get_known_peers(self) -> Iterable[Peer]:
        """
        Get all peers known to be part of the conflict set
        Returns: list of known peers in the sub-community
        """
        pass

    def get_known_peers_ids(self) -> List[bytes]:
        return sorted(p.mid for p in self.get_known_peers())

    @abstractmethod
    def add_peer(self, peer: Peer):
        pass

    @abstractmethod
    async def unload(self):
        pass


class IPv8ConflictSet(Community, BaseConflictSet):
    def get_known_peers(self) -> Iterable[Peer]:
        return self.get_peers()

    @property
    def cs_id(self) -> bytes:
        return self._subcom_id

    def __init__(self, *args, **kwargs):
        self._subcom_id = kwargs.pop("cs_id")
        self._peer_store = kwargs.pop("peer_store")
        self.community_id = self.cs_id
        super().__init__(*args, **kwargs)

    def add_peer(self, peer: Peer):
        self._peer_store.add_peer_state_id(self._subcom_id, peer.mid)
        self.network.add_verified_peer(peer)
        self.network.discover_services(peer, [self.community_id])

    def introduction_response_callback(self, peer, dist, payload):
        self._peer_store.add_peer_state_id(self._subcom_id, peer.mid)

    def introduction_request_callback(self, peer, dist, payload):
        self._peer_store.add_peer_state_id(self._subcom_id, peer.mid)


class LightConflictSet(BaseConflictSet):
    async def unload(self):
        pass

    def __init__(self, cs_id: bytes = None, max_peers: int = None, **kwargs):
        self._subcom_id = cs_id
        self.peers = set()
        self._peer_store: BasePeerStore = kwargs.pop("peer_store")

    @property
    def cs_id(self) -> bytes:
        return self._subcom_id

    def get_known_peers(self) -> Iterable[Peer]:
        return self.peers

    def add_peer(self, peer: Peer):
        self._peer_store.add_peer_state_id(self._subcom_id, peer.mid)
        self.peers.add(peer)


class BaseConflictSetFactory(ABC):
    @abstractmethod
    def create_conflict_set(self, *args, **kwargs) -> BaseConflictSet:
        pass


class IPv8ConflictSetFactory(BaseConflictSetFactory):
    def create_conflict_set(self, *args, **kwargs) -> BaseConflictSet:
        """
        Args:
            subcom_id: id of the community
            max_peers: maximum number of peer to connect to in the community
        Returns:
            SubCommunity as an IPv8 community
        """
        if not self.ipv8:
            raise UnavailableIPv8Exception("Cannot create subcommunity without IPv8")
        else:
            subcom = IPv8ConflictSet(
                self.my_peer, self.ipv8.endpoint, self.network, *args, **kwargs
            )
            self.ipv8.overlays.append(subcom)
            return subcom


class LightConflictSetFactory(BaseConflictSetFactory):
    @staticmethod
    def create_conflict_set(*args, **kwargs) -> BaseConflictSet:
        """
        Args:
            subcom_id: id of the community
        Returns:
            SubCommunity as a LightCommunity (just set of peers)
        """
        return LightConflictSet(*args, **kwargs)


class SubCommunityRoutines(ABC):
    @property
    @abstractmethod
    def my_conflict_sets(self) -> Iterable[bytes]:
        """
        All sub-communities that my peer is part of
        Returns: list with sub-community ids
        """
        pass

    @abstractmethod
    def discovered_peers_by_conflict_set(self, cs_id: bytes) -> Iterable[Peer]:
        pass

    @abstractmethod
    def get_conflict_set(self, cs_id: bytes) -> Optional[BaseConflictSet]:
        pass

    @abstractmethod
    def add_conflict_set(self, sub_com: bytes, subcom_obj: BaseConflictSet) -> None:
        """
        Add/Store conflict set
        Args:
            sub_com: sub-community identifier
            subcom_obj: SubCommunity object
        """
        pass

    @abstractmethod
    def notify_peers_on_new_conflict_set(self) -> None:
        """Notify other peers on updates of the sub-communities"""
        pass

    @property
    @abstractmethod
    def cs_factory(
        self,
    ) -> Union[BaseConflictSetFactory, Type[BaseConflictSetFactory]]:
        """Factory for creating sub-communities"""
        pass

    @abstractmethod
    def on_join_cs(self, cs_id: bytes) -> None:
        """
        Procedure on joining new conflict set
        Args:
            cs_id: conflict set id
        """
        pass


class ConflictSetsMixin(SubCommunityRoutines, CommunityRoutines, metaclass=ABCMeta):
    # def  structured_conflict_set(self) -> :

    # Structured conflict set
    def get_structured_conflict_set(self, state_id: bytes):
        cs = self.get_conflict_set(state_id)
        kp = cs.get_known_peers_ids()
        kp.append(self.my_peer.mid)
        kp = sorted(kp)
        n = len(kp)

        sn = self.settings.sync_neighbours(n)
        s = n // sn
        # Fast links
        links = []
        for i in range(1, sn):
            links.append(i * s)
        return kp, links

    @abstractmethod
    def get_next_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass

    @abstractmethod
    def get_prev_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass

    def is_subscribed(self, community_id: bytes) -> bool:
        return community_id in self.my_conflict_sets

    def join_cs(self, cs_id: bytes, discovery_params: Dict[str, Any] = None) -> None:
        if cs_id not in self.my_conflict_sets:
            # New conflict set community
            # Create conflict set object
            subcom = self.cs_factory.create_conflict_set(
                cs_id=cs_id,
                max_peers=self.settings.subcom_max_peers,
                peer_store=self.peer_store,
            )
            self.add_conflict_set(cs_id, subcom)
            # Call discovery routine for this sub-community
            for p in self.discovered_peers_by_conflict_set(cs_id):
                subcom.add_peer(p)
            self.discovery_strategy.discover(
                subcom,
                target_peers=self.settings.subcom_min_peers,
                discovery_params=discovery_params,
            )

    def subscribe_to_conflict_sets(
        self, subcoms: Iterable[bytes], discovery_params: Dict[str, Any] = None
    ) -> None:
        """
        Subscribe to the sub communities with given ids.

        If bootstrap_master is not specified, we will use RandomWalks to discover other peers within the same community.
        A peer will connect to at most `settings.max_peers_subtrust` peers.
        Args:
            subcoms: Iterable object with sub_community ids
            discovery_params: Dict parameters for the discovery process
        """
        updated = False
        for c_id in subcoms:
            if c_id not in self.my_conflict_sets:
                self.join_cs(c_id, discovery_params)
                # Join the sub-community for auditing, new transactions
                self.on_join_cs(c_id)
                updated = True
        if updated:
            self.notify_peers_on_new_conflict_set()

    def subscribe_to_conflict_set(
        self, subcom_id: bytes, discovery_params: Dict[str, Any] = None
    ) -> None:
        """
        Subscribe to the SubCommunity with the public key master peer.
        Community is identified with a community_id.

        Args:
            subcom_id: bytes identifier of the community
            discovery_params: Dict parameters for the discovery process
        """
        if subcom_id not in self.my_conflict_sets:
            self.join_cs(subcom_id, discovery_params)
            # Join the protocol audits/ updates
            self.on_join_cs(subcom_id)
            # Notify other peers that you are part of the new community
            self.notify_peers_on_new_conflict_set()
