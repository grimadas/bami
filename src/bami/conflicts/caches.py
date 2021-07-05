import logging
import random
from typing import Callable

from bami.backbone.utils import Notifier
from bami.conflicts.peer_management import PeerStatus
from ipv8.peer import Peer
from ipv8.requestcache import NumberCache, RequestCache


class PeerSyncCache(NumberCache):
    """
    Cache tied to a peer (mid).
    """

    def __init__(
        self,
        request_cache: RequestCache,
        notifier: Notifier,
        prefix: str,
        peer: Peer,
        state_id: bytes,
        max_tries: int,
        retry_func: Callable[[Peer, bytes, int], None],
        min_timeout_delay: float = 1.00,
        max_timeout_delay: float = 2.00,
        request_delay: float = 0,
    ):
        super().__init__(request_cache, prefix, hash(peer))
        self.request_cache = request_cache
        self.peer = peer
        self.state_id = state_id
        self.max_tries = max_tries
        self.notifier = notifier
        self.retry_func = retry_func
        self.logger = logging.getLogger(type(self).__name__)

        self.min_t_delay = min_timeout_delay
        self.max_t_delay = max_timeout_delay
        self.request_delay = request_delay

    def on_timeout(self) -> float:
        if self.max_tries < 1:
            self.notifier.notify(PeerStatus.SUSPECTED, self.peer, self.state_id)
            return

        async def retry_later():
            try:
                self.retry_func(
                    self.peer, self.state_id, self.max_tries,
                )
            except Exception as e:
                self.logger.info("Error encountered during 'on_timeout' (error: %s)", e)

        self.request_cache.register_anonymous_task(
            "retry-later-" + hash(self.peer), retry_later, delay=self.request_delay
        )

    @property
    def timeout_delay(self):
        return self.min_t_delay + random.random() * (
            self.max_t_delay - self.min_t_delay
        )
