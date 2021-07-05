from binascii import hexlify
from hashlib import sha256
from itertools import chain
import logging
from typing import Any, Callable, List, NewType, Set, Tuple

from bami.sync.data_models import Link
from msgpack import dumps, loads
import numpy as np

KEY_LEN = 8
ShortKey = NewType("ShortKey", bytes)
BytesLinks = NewType("BytesLinks", bytes)
Dot = NewType("Dot", Tuple[int, ShortKey])
Links = NewType("Links", Tuple[Tuple[int, ShortKey]])
Ranges = NewType("Ranges", Tuple[Tuple[int, int]])
StateVote = NewType("StateVote", Tuple[bytes, bytes, bytes])

PackableCells = NewType("PackableCells", Tuple[int, List[int]])
CellsArray = NewType("CellsArray", np.ndarray)


GENESIS_HASH = b"0" * 32  # ID of the first tx of the chain.
GENESIS_SEQ = 1
UNKNOWN_SEQ = 0
EMPTY_SIG = b"0" * 64
EMPTY_PK = b"0" * 74
ANY_COUNTERPARTY_PK = EMPTY_PK


AUDIT_TYPE = b"audit"
CONFIRM_TYPE = b"confirm"
REJECT_TYPE = b"reject"


def shorten(key: bytes) -> ShortKey:
    return ShortKey(key[-KEY_LEN:])


GENESIS_DOT = Dot((GENESIS_SEQ - 1, shorten(GENESIS_HASH)))
GENESIS_LINK = Links(((GENESIS_SEQ - 1, shorten(GENESIS_HASH)),))
GEN_LINK = Link(0, shorten(GENESIS_HASH))
GEN_DOT = (0, shorten(GENESIS_HASH))


def hex_to_int(hex_val) -> int:
    return int(hexlify(hex_val), 16) % 100000000


def short_to_int(short_key: ShortKey) -> int:
    return int(short_key, 16)


def int_to_short_key(int_val: int) -> ShortKey:
    val = hex(int_val)[2:]
    while len(val) < KEY_LEN:
        val = "0" + val
    return ShortKey(val)


def wrap_return(gener_val):
    return list(chain(*(k for k in gener_val)))


def wrap_iterate(gener_val):
    for _ in gener_val:
        pass


def take_hash(value: Any) -> bytes:
    """Encode to bytes and return hash digest

    Args:
        value: python object with __repr__

    Returns:
        Hash in bytes
    """
    return sha256(encode_raw(value)).digest()


def encode_raw(val: Any) -> bytes:
    """Encode python object to bytes"""
    return dumps(val)


def decode_raw(byte_raw: bytes) -> Any:
    """Decode bytes to python struct"""
    return loads(byte_raw, strict_map_key=False, use_list=False)


def encode_links(link_val: Links) -> BytesLinks:
    """Encode to the sendable payload_cls
    Args:
        link_val:

    Returns:
        creator_state encoded in bytes
    """
    return BytesLinks(encode_raw(link_val))


def decode_links(bytes_links: BytesLinks) -> Links:
    """Decode bytes to creator_state

    Args:
        bytes_links: bytes containing creator_state delta

    Returns:
        Links values
    """
    return Links(tuple(tuple(x) for x in decode_raw(bytes_links)))


def expand_ranges(range_vals: Ranges) -> Set[int]:
    """Expand ranges to set of ints

    Args:
        range_vals: List of tuple ranges

    Returns:
        Set of ints with ranges expanded
    """
    val_set = set()
    for b, e in range_vals:
        for val in range(b, e + 1):
            val_set.add(val)
    return val_set


def ranges(nums: Set[int]) -> Ranges:
    """Compress numbers to tuples of consequent ranges

    Args:
        nums: set of numbers

    Returns:
        List of tuples of ranges
    """
    if not nums:
        return Ranges(tuple())
    nums = sorted(nums)
    gaps = [[s, e] for s, e in zip(nums, nums[1:]) if s + 1 < e]
    edges = iter(nums[:1] + sum(gaps, []) + nums[-1:])
    return Ranges(tuple(zip(edges, edges)))


class Notifier(object):
    def __init__(self):
        self.observers = {}
        self.logger = logging.getLogger("Notifier")

    def add_unique_observer(self, subject: Any, callback: Callable) -> None:
        self.observers[subject] = [callback]

    def add_observer(self, subject: Any, callback: Callable) -> None:
        self.observers[subject] = self.observers.get(subject, [])
        self.observers[subject].append(callback)

    def notify(self, subject: Any, *args, **kwargs) -> None:
        if subject not in self.observers:
            return
        for callback in self.observers[subject]:
            callback(*args, **kwargs)
