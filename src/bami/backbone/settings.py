from enum import Enum
from typing import Callable

import numpy as np


class SecurityMode(Enum):
    """
    Implementations of security implementations of Trustchain.
    """

    VANILLA = 1
    AUDIT = 2
    BOTH = 3


class SyncMode(Enum):
    """
    Gossip synchronization modes
    """

    BLOCK_SYNC = 1
    STATE_SYNC = 2
    FULL_SYNC = 3
    PASSIVE = 4


class BamiSettings(object):
    """
    This class holds various settings regarding TrustChain.
    """

    def __init__(self):
        # Push gossip properties: fanout and ttl (number of hops)
        self.push_gossip_fanout = -1
        # Request delay
        self.min_request_delay = 0.3
        self.max_request_delay = 0.6
        self.max_request_tries = 5

        # Whether frontier gossip is enabled
        self.peer_sync_enabled = True
        # Maximum delay before starting the frontier sync in the community
        self.state_sync_min_delay = 0.2
        self.state_sync_max_delay = 0.4
        # The interval at which we gossip the latest frontier in each community

        self.frontier_gossip_interval = 0.5
        # The waiting time between processing two collected frontiers
        self.frontier_gossip_collect_time = 0.2
        # Gossip fanout for frontiers exchange
        self.sync_neighbours: Callable[[int], int] = lambda n: int(np.log2(n)) if n > 7 else n
        self.block_sign_delta = 0.3

        # working directory for the database
        self.work_directory = ".block_db"
        self.peer_directory = ".peers_db"

        # The maximum and minimum number of peers in the main communities
        self.main_min_peers = 20
        self.main_max_peers = 30

        # The maximum and minimum number of peers in sub-communities
        self.subcom_min_peers = 20
        self.subcom_max_peers = 30
