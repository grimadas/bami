from dataclasses import dataclass
from typing import List

from bami.backbone.payload import enrich, msg_payload, payload
from ipv8.messaging.lazy_payload import VariablePayload


@payload
@dataclass
class FilterPack:
    filter_bits: bytes
    seed: int


@payload
@dataclass
class CellState:
    cell_additive: int
    cells: bytes


@payload
@dataclass(unsafe_hash=True)
class Link:
    tx_index: int
    tx_hash: bytes


@msg_payload
@dataclass
class PeerState:
    cells: CellState
    joined_hash: bytes
    state_id: bytes
    opt_bloom_filter: FilterPack


@enrich
class RawPeerState(VariablePayload):
    names = ["value"]
    format_list = ["raw"]


@msg_payload
@dataclass
class PeerStateRequest:
    signed_peer_state: bytes
    requested_peers: List[bytes]


@msg_payload
@dataclass
class PeersConflictSet:
    conflict_set: List[bytes]


@msg_payload
@dataclass
class InconsistentStatePayload:
    state: bytes
