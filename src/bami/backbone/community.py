"""
The Plexus backbone
"""
from abc import ABCMeta
from asyncio import (
    ensure_future,
    Future,
    iscoroutinefunction,
    sleep,
    Task,
)
from binascii import hexlify, unhexlify
import random
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from bami.backbone.community_routines import MessageStateMachine
from bami.backbone.discovery import (
    RandomWalkDiscoveryStrategy,
    SubCommunityDiscoveryStrategy,
)
from bami.backbone.exceptions import (
    IPv8UnavailableException,
    SubCommunityEmptyException,
)
from bami.backbone.settings import BamiSettings
from bami.backbone.transaction_sync import TransactionSyncMixin
from bami.backbone.utils import (
    decode_raw,
    encode_raw,
    Notifier,
)
from bami.conflicts.conflict_set import (
    BaseConflictSet,
    BaseConflictSetFactory,
    ConflictSetsMixin,
)
from bami.datastore.database import BaseDB, DBManager
from bami.datastore.peer_store import BasePeerStore, PeerStore
from bami.sync.data_models import PeersConflictSet
from bami.sync.state_sync import PeerStateSyncMixin
from ipv8.community import Community
from ipv8.keyvault.keys import Key
from ipv8.lazy_community import lazy_wrapper
from ipv8.messaging.payload import Payload
from ipv8.peer import Address, Peer
from ipv8.peerdiscovery.discovery import EdgeWalk, RandomWalk
from ipv8.peerdiscovery.network import Network
from ipv8.requestcache import RequestCache
from ipv8.util import coroutine
from ipv8_service import IPv8


# TransactionSyncMixin


class BamiCommunity(
    Community,
    PeerStateSyncMixin,
    TransactionSyncMixin,
    ConflictSetsMixin,
    BaseConflictSetFactory,
    metaclass=ABCMeta,
):
    #
    """
    Community for secure backbone.
    """

    community_id = unhexlify("5789fd5555d60c425694ac139d1d8d7ea37d009e")
    version = b"\x04"

    async def flex_runner(
        self,
        delay: Callable[[], float],
        interval: Callable[[], float],
        task: Callable,
        *args: List
    ) -> None:
        await sleep(delay())
        while True:
            await task(*args)
            await sleep(interval())

    def register_flexible_task(
        self,
        name: str,
        task: Callable,
        *args: List,
        delay: Callable = None,
        interval: Callable = None
    ) -> Union[Future, Task]:
        """
        Register a Task/(coroutine)function so it can be canceled at shutdown time or by name.
        """
        if not delay:

            def delay():
                return random.random()

        if not interval:

            def interval():
                return random.random()

        task = task if iscoroutinefunction(task) else coroutine(task)
        return self.register_task(
            name, ensure_future(self.flex_runner(delay, interval, task, *args))
        )

    def __init__(
        self,
        my_peer: Peer,
        endpoint: Any,
        network: Network,
        ipv8: Optional[IPv8] = None,
        max_peers: int = None,
        anonymize: bool = False,
        db: Tuple[BaseDB, BasePeerStore] = None,
        work_dirs: Tuple[str, str] = None,
        discovery_strategy: SubCommunityDiscoveryStrategy = None,
        settings: BamiSettings = None,
        **kwargs
    ):
        if not settings:
            self._settings = BamiSettings()
        else:
            self._settings = settings

        if not work_dirs:
            work_dirs = self.settings.work_directory, self.settings.peer_directory
        if not db:
            self._persistence = DBManager(work_dirs[0])
            self._peers_store = PeerStore(work_dirs[1])
        else:
            self._persistence, self._peers_store = db
        if not max_peers:
            max_peers = self.settings.main_max_peers
        self._ipv8 = ipv8

        self.discovery_strategy = discovery_strategy
        if not self.discovery_strategy:
            self.discovery_strategy = RandomWalkDiscoveryStrategy(self._ipv8)

        super(BamiCommunity, self).__init__(
            my_peer, endpoint, network, max_peers, anonymize=anonymize
        )

        # Create DB Manager

        self.logger.debug(
            "🍜 community started with 🆔: %s", hexlify(self.my_pub_key_bin),
        )
        self.relayed_broadcasts = set()

        self.shutting_down = False

        # Sub-Communities logic
        self.my_subscriptions = dict()

        self.peer_subscriptions = (
            dict()
        )  # keeps track of which communities each peer is part of
        self.bootstrap_master = None

        self.periodic_sync_lc = {}

        self._request_cache = RequestCache()

        self.ordered_notifier = Notifier()
        self.unordered_notifier = Notifier()

        self.structure_links: Dict[bytes, List[int]] = {}
        self.next_peers: Dict[bytes, List[Peer]] = {}
        self.prev_peers: Dict[bytes, List[Peer]] = {}

        # Setup and add message handlers
        for base in BamiCommunity.__bases__:
            if issubclass(base, MessageStateMachine):
                base.setup_messages(self)

        for base in self.__class__.__mro__:
            if "setup_mixin" in base.__dict__.keys():
                base.setup_mixin(self)

        self.add_message_handler(PeersConflictSet, self.received_peer_subs)

    def on_join_cs(self, cs_id: bytes) -> None:
        """
        This method is called when BAMI joins a new sub-community.
        We enable some functionality by default, but custom behaviour can be added by overriding this method.

        Args:
            cs_id: The ID of the sub-community we just joined.

        """
        self._persistence.init_chain_engine(cs_id)
        if self.settings.peer_sync_enabled:
            # Start exchanging frontiers
            self.start_peer_sync(cs_id)

    # ----- Discovery start -----
    def start_discovery(
        self,
        target_peers: int = None,
        discovery_algorithm: Union[Type[RandomWalk], Type[EdgeWalk]] = RandomWalk,
        discovery_params: Dict[str, Any] = None,
    ):

        if not self._ipv8:
            raise IPv8UnavailableException("Cannot start discovery at main community")

        discovery = (
            discovery_algorithm(self)
            if not discovery_params
            else discovery_algorithm(self, **discovery_params)
        )
        if not target_peers:
            target_peers = self.settings.main_min_peers
        self._ipv8.add_strategy(self, discovery, target_peers)

    def create_introduction_request(
        self, socket_address, extra_bytes=b"", new_style=False, prefix=None
    ):
        extra_bytes = encode_raw(tuple(self.my_conflict_sets))
        return super().create_introduction_request(
            socket_address, extra_bytes, new_style
        )

    def create_introduction_response(
        self,
        lan_socket_address,
        socket_address,
        identifier,
        introduction=None,
        extra_bytes=b"",
        prefix=None,
        new_style=False,
    ):
        extra_bytes = encode_raw(tuple(self.my_conflict_sets))
        return super().create_introduction_response(
            lan_socket_address,
            socket_address,
            identifier,
            introduction,
            extra_bytes,
            prefix,
            new_style,
        )

    # ---- Introduction handshakes => Exchange your subscriptions ----------------

    def introduction_response_callback(self, peer, dist, payload):
        subcoms = decode_raw(payload.extra_bytes)
        self.process_peer_subscriptions(peer, subcoms)
        # TODO: add subscription strategy

    def introduction_request_callback(self, peer, dist, payload):
        subcoms = decode_raw(payload.extra_bytes)
        self.process_peer_subscriptions(peer, subcoms)

    @property
    def peer_store(self) -> BasePeerStore:
        return self._peers_store

    @property
    def request_cache(self) -> RequestCache:
        return self._request_cache

    # ----- Community and PeerManagement routines ------

    async def unload(self):
        self.logger.debug("Unloading the 🍜 community.")
        self.shutting_down = True

        for base in self.__class__.__mro__:
            if "unload_mixin" in base.__dict__.keys():
                base.unload_mixin(self)

        for subcom_id in self.my_subscriptions:
            await self.my_subscriptions[subcom_id].unload()
        await super(BamiCommunity, self).unload()
        await self._request_cache.shutdown()

        # Close the persistence layer
        self.persistence.close()

    @property
    def settings(self) -> BamiSettings:
        return self._settings

    @property
    def persistence(self) -> BaseDB:
        return self._persistence

    def subscribe_on_ordered_updates(self, topic: bytes, callback):
        # initialize processing engine
        self.logger.debug('Subscribed on %s', topic)
        self._persistence.order_tx.add_observer(topic, callback)

    @property
    def my_pub_key_bin(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    def prepare_packet(self, payload: Payload, sig: bool = True) -> bytes:
        return self.ezr_pack(payload.msg_id, payload, sig=sig)

    def send_packet(self, address: Address, packet: bytes) -> None:
        self.endpoint.send(address, packet)

    def send_payload(self, peer: Peer, payload: Payload, sig: bool = True) -> None:
        self.ez_send(peer, payload, sig=sig)

    @property
    def my_peer_key(self) -> Key:
        return self.my_peer.key

    # ----- SubCommunity routines ------

    @property
    def cs_factory(
        self,
    ) -> Union[BaseConflictSetFactory, Type[BaseConflictSetFactory]]:
        return self

    @property
    def my_conflict_sets(self) -> Iterable[bytes]:
        return set(self.my_subscriptions.keys())

    @property
    def my_conflict_sets_payload(self) -> Payload:
        return PeersConflictSet(self.my_conflict_sets)

    def get_conflict_set(self, subcom_id: bytes) -> Optional[BaseConflictSet]:
        return self.my_subscriptions.get(subcom_id)

    def add_conflict_set(self, sub_com: bytes, subcom_obj: BaseConflictSet) -> None:
        if not subcom_obj:
            raise SubCommunityEmptyException("Sub-Community object is none", sub_com)
        self.my_subscriptions[sub_com] = subcom_obj

    def discovered_peers_by_conflict_set(self, subcom_id: bytes) -> Iterable[Peer]:
        return self.peer_subscriptions.get(subcom_id, [])

    def process_peer_subscriptions(self, peer: Peer, subcoms: Set[bytes]) -> None:
        for c in subcoms:
            # For each sub-community that is also known to me - introduce peer.
            if c in self.my_subscriptions:
                self.my_subscriptions[c].add_peer(peer)
                self.restructure_conflict_set(c)
            # Keep all sub-communities and peer in a map
            if c not in self.peer_subscriptions:
                self.peer_subscriptions[c] = set()
            self.peer_subscriptions[c].add(peer)

    @lazy_wrapper(PeersConflictSet)
    def received_peer_subs(self, peer: Peer, payload: PeersConflictSet) -> None:
        self.process_peer_subscriptions(peer, payload.conflict_set)

    def restructure_conflict_set(self, state_id: bytes) -> None:
        cp, sl = self.get_structured_conflict_set(state_id)
        self.structure_links[state_id] = sl

        my_mid = self.my_peer.mid
        n = len(cp)
        pi = cp.index(my_mid)

        np = [(i + pi) % n for i in sl]
        np.append((pi + 1) % n)
        np = set(np)
        self.next_peers[state_id] = [self.get_peer_by_mid(cp[i], state_id) for i in np]

        np = [(pi - i) % n for i in sl]
        np.append((pi + 1) % n)
        np = set(np)
        self.prev_peers[state_id] = [self.get_peer_by_mid(cp[i], state_id) for i in np]

    def get_next_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        return self.next_peers[state_id]

    def get_prev_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        return self.prev_peers[state_id]

    def notify_peers_on_new_conflict_set(self) -> None:
        for peer in self.get_peers():
            self.send_payload(peer, self.my_conflict_sets_payload)

    # --- Keeping the state of peers updated -----

    def start_peer_sync(
        self,
        subcom_id: bytes,
        delay: Callable[[], float] = None,
        interval: Callable[[], float] = None,
    ) -> None:
        self.logger.debug("Sync ⚙️ on️ %s", subcom_id)

        self.periodic_sync_lc[subcom_id] = self.register_flexible_task(
            "gossip-sync-" + str(subcom_id),
            self.peer_sync_task,
            subcom_id,
            delay=delay
            if delay
            else lambda: self._settings.state_sync_min_delay
            + random.random() * self._settings.state_sync_max_delay,
            interval=interval
            if interval
            else lambda: self._settings.state_sync_min_delay
            + random.random() * self._settings.state_sync_max_delay,
        )

    def get_peer_by_key(
        self, peer_key: bytes, subcom_id: bytes = None
    ) -> Optional[Peer]:
        if subcom_id:
            subcom_peers = self.get_conflict_set(subcom_id).get_known_peers()
            for peer in subcom_peers:
                if peer.public_key.key_to_bin() == peer_key:
                    return peer
        for peer in self.get_peers():
            if peer.creator_id.key_to_bin() == peer_key:
                return peer
        return None

    def get_peer_by_mid(self, peer_mid: bytes, subcom_id: bytes) -> Optional[Peer]:
        if subcom_id:
            subcom_peers = self.get_conflict_set(subcom_id).get_known_peers()
            for peer in subcom_peers:
                if peer.mid == peer_mid:
                    return peer
        for peer in self.get_peers():
            if peer.mid == peer_mid:
                return peer
        return None

    @staticmethod
    def choose_community_peers(
        com_peers: Iterable[Peer], current_seed: Any, commitee_size: int
    ) -> Iterable[Peer]:
        rand = random.Random(current_seed)
        commitee_size = len(com_peers) if commitee_size == -1 else commitee_size
        return rand.sample(list(com_peers), min(commitee_size, len(com_peers)))

    def share_in_community(
        self,
        packet: Union[Payload, bytes],
        subcom_id: bytes = None,
        fanout: int = None,
        seed: Any = None,
    ) -> None:
        """
        Broadcast/Gossip in the community
        """
        if not subcom_id or not self.get_conflict_set(subcom_id):
            # Share to every known peer
            peers = self.get_peers()
        else:
            peers = self.get_conflict_set(subcom_id).get_known_peers()
        if not seed:
            seed = random.random()
        if not fanout:
            fanout = self.settings.push_gossip_fanout
        if peers:
            selected_peers = self.choose_community_peers(peers, seed, fanout)
            if type(packet) == bytes:
                for p in selected_peers:
                    self.send_packet(p.address, packet)
            else:
                for p in selected_peers:
                    self.send_payload(p, packet)
        else:
            raise SubCommunityEmptyException(
                "No peers to share with com id: {}".format(subcom_id)
            )


class BamiTestnetCommunity(BamiCommunity, metaclass=ABCMeta):
    """
    This community defines the testnet for Plexus
    """

    DB_NAME = "plexus_testnet"
    community_id = unhexlify("4dcfcf5bacc89aa5af93ef8a695580c9ddf1f1a0")
