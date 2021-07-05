from __future__ import annotations

from binascii import hexlify
from collections import defaultdict
import hashlib
import logging
from typing import Any, Iterator, List, Optional, Set, Tuple

from bami.sync.data_models import FilterPack
from bami.sync.exceptions import DesynchronizedBloomFiltersException
from bami.sync.peer_state import CellIndexer
import mmh3 as mmh3
import numpy as np


def filter_id(state_id: bytes, counter_party_id: bytes) -> str:
    return hashlib.md5(state_id + counter_party_id).hexdigest()


def bitarray_to_bytes(bit_array: np.ndarray) -> bytes:
    return np.packbits(bit_array).tobytes()


def bytes_to_bitarray(array_bytes: bytes) -> np.ndarray:
    return np.unpackbits(np.frombuffer(array_bytes, dtype=np.uint8))


def both_present(bf1_array: np.ndarray, bf2_array: np.ndarray) -> np.ndarray:
    return np.where((bf1_array > 0) & (bf2_array > 0))[0]


class TxIndexer:
    ind2txs = defaultdict(lambda: defaultdict(lambda: set()))
    filter_txs = defaultdict(lambda: dict())
    unprocessed_txs = defaultdict(lambda: set())

    @classmethod
    def add_new_tx(cls, f_id: str, tx_hash: int) -> None:
        cls.unprocessed_txs[f_id].add(tx_hash)

    @classmethod
    def reinit(cls):
        cls.filter_txs = defaultdict(lambda: dict())
        cls.unprocessed_txs = defaultdict(lambda: set())

    @classmethod
    def get_tx_by_index(cls, f_id: str, index: int) -> Set[Any]:
        return cls.ind2txs[f_id][index]

    @classmethod
    def add_to_index(cls, tx_hash: int, f_id: str, indexes: Set[int]) -> None:
        cls.filter_txs[f_id][tx_hash] = indexes
        for i in indexes:
            cls.ind2txs[f_id][i].add(tx_hash)


class BloomFilter:
    def create_hash_func(self, num_funcs: int = 1, seed_value: int = 0) -> None:
        for k in range(num_funcs):
            self.hash_funcs.append(mmh3.hash128(bytes(k), seed=seed_value))

    def get_indices(self, hash_val: int) -> Iterator[int]:
        bit_count = len(self.bits)
        for h in self.hash_funcs:
            i = (h ^ hash_val) % bit_count
            yield i

    def __init__(
        self, block_size: int, seed_value: int = 0, num_funcs: int = 1,
    ) -> None:
        super().__init__()
        self.init_filter(block_size, seed_value, num_funcs)

    @property
    def capacity(self) -> float:
        return 1 - sum(self.bits) / len(self.bits)

    def __str__(self) -> str:
        return "🔮 BloomFilter: seed: {}, cap: {}".format(self.seed_value, self.capacity)

    def init_filter(self, m: int, seed_value: int, num_funcs: int) -> None:
        self.bits = np.array([0] * m)
        self.seed_value = seed_value
        self.num_funcs = num_funcs
        self.hash_funcs = []
        self.create_hash_func(num_funcs, seed_value)

    def reindex(
        self,
        new_seed_value: int = None,
        new_size: int = None,
        new_num_funcs: int = None,
    ) -> None:
        seed = new_seed_value if new_seed_value else self.seed_value
        m = new_size if new_size else len(self.bits)
        num_func = new_num_funcs if new_num_funcs else self.num_funcs
        self.init_filter(m, seed, num_func)

    def add(self, hash_val: int) -> List[int]:
        indexes = []
        for i in self.get_indices(hash_val):
            self.bits[i] = 1
            indexes.append(i)
        return indexes

    def maybe_element(self, obj_val: int) -> bool:
        for index in self.get_indices(obj_val):
            if self.bits[index] == 0:
                return False
        return True

    def to_filter_pack(self) -> FilterPack:
        return FilterPack(bitarray_to_bytes(self.bits), self.seed_value)


class BloomFiltersManager:
    def __init__(self, my_peer_id: bytes, size=8 * 100, num_funcs=3):
        self.filters = {}
        self.num_funcs = num_funcs
        self.size = size
        self.my_peer_id = int(hexlify(my_peer_id), 16)
        self.MOD_VAL = 255
        self.logger = logging.getLogger("BloomFiltersManager")

        TxIndexer.reinit()

    def get_init_seed(self, counter_party_id: bytes) -> int:
        c = hexlify(counter_party_id)
        init_seed_value = int(c, 16) + self.my_peer_id + 0
        init_seed_value = init_seed_value % self.MOD_VAL
        return init_seed_value

    def bloom_filter(self, state_id: bytes, counter_party_id: bytes) -> BloomFilter:
        # Fill the bloom filter with the known transactions.
        f_id = filter_id(state_id, counter_party_id)
        val = self.filters.get(f_id)
        if not val:
            val = BloomFilter(
                block_size=self.size,
                num_funcs=self.num_funcs,
                seed_value=self.get_init_seed(counter_party_id),
            )
            self.filters[f_id] = val
            self.logger.debug(
                "Created: %s at %s, %s",
                self.filters[f_id],
                int(hexlify(counter_party_id), 16),
                self.my_peer_id,
            )
        return val

    def iter_seed_value(self, old_seed_value: int) -> int:
        return (old_seed_value + 1) % self.MOD_VAL

    def add_to_bloom_index(
        self, state_id: bytes, counter_party_id: bytes, tx_hash: int
    ) -> None:
        f_id = filter_id(state_id, counter_party_id)
        ind = self.bloom_filter(state_id, counter_party_id).add(tx_hash)
        TxIndexer.filter_txs[f_id][tx_hash] = set(ind)


def iterate_bloom_filter(
    blm_filter_manager: BloomFiltersManager,
    state_diff: np.ndarray,
    state_id: bytes,
    counter_party_id: bytes,
    last_peer_bf: Optional[FilterPack],
) -> Tuple[Set[bytes], Set[bytes]]:
    f_id = filter_id(state_id, counter_party_id)
    all_txs = TxIndexer.filter_txs[f_id]
    unprocessed_txs = TxIndexer.unprocessed_txs[f_id]
    if not last_peer_bf:
        return set(all_txs) | TxIndexer.unprocessed_txs[f_id], set()
    my_blm = blm_filter_manager.bloom_filter(state_id, counter_party_id)

    my_bf_bits = my_blm.bits
    peer_bits = bytes_to_bitarray(last_peer_bf.filter_bits)

    if last_peer_bf.seed != my_blm.seed_value:
        raise DesynchronizedBloomFiltersException(
            "Seeds: {} != {}, Peer: {}".format(
                last_peer_bf.seed, my_blm.seed_value, "peer"
            )
        )

    bf_indexes = both_present(my_bf_bits, peer_bits)
    tx_to_remove = set()
    for t_id, indexes in all_txs.items():
        c_id = CellIndexer.cell_id(t_id)
        if indexes.issubset(bf_indexes) and state_diff[c_id] > 0:
            # Add transaction to remove
            state_diff[c_id] -= 1
            tx_to_remove.add(t_id)
    blm_filter_manager.logger.debug(
        "Unprocessed txs %s", TxIndexer.unprocessed_txs[f_id]
    )
    new_txs = (set(all_txs) | TxIndexer.unprocessed_txs[f_id]) - tx_to_remove
    blm_filter_manager.logger.debug("New txs %s", new_txs)
    # Refill by bloom filter with new transactions
    my_blm.reindex(new_seed_value=blm_filter_manager.iter_seed_value(my_blm.seed_value))
    tx_inds = {}
    for t in new_txs:
        tx_inds[t] = my_blm.add(t)
    TxIndexer.filter_txs[f_id] = tx_inds
    TxIndexer.unprocessed_txs[f_id] = TxIndexer.unprocessed_txs[f_id] - new_txs
    blm_filter_manager.logger.debug("Iterate: %s", my_blm)
    return new_txs, tx_to_remove
