# Peer state operations
from typing import Optional

from bami.backbone.utils import CellsArray
from bami.sync.data_models import CellState, PeerState
import numpy as np


class CellIndexer:
    max_hash = 100000000
    n_cells = 32
    cell_refptr = max_hash // n_cells + 1

    @classmethod
    def cell_id(cls, tx_hash: int) -> int:
        return tx_hash // cls.cell_refptr

    @classmethod
    def create_cell_array(cls) -> CellsArray:
        return np.array([0] * cls.n_cells, dtype=np.int_)

    @classmethod
    def update_cell_state(cls, tx_hash: int, cell_state: CellState) -> CellState:
        ca = cls.to_cells_array(cell_state)
        ca[cls.cell_id(tx_hash)] += 1
        return cls.to_cells_array(ca)

    @staticmethod
    def to_packable_cells(cells_array: CellsArray) -> CellState:
        count = min(cells_array)
        c = cells_array - count
        return CellState(count, c.tobytes())

    @staticmethod
    def to_cells_array(packable_cells: CellState) -> CellsArray:
        return CellsArray(
            np.frombuffer(packable_cells.cells, np.int_) + packable_cells.cell_additive
        )


def estimate_peer_diff(state_1: PeerState, state_2: Optional[PeerState]) -> CellsArray:
    """
    Estimate state difference
    @return: State diff array
    """
    sc1 = CellIndexer.to_cells_array(state_1.cells)
    if not state_2:
        return sc1
    sc2 = CellIndexer.to_cells_array(state_2.cells)
    return sc1 - sc2


def is_state_diff_inconsistent(state_diff: CellsArray) -> bool:
    return np.any(state_diff < 0) and np.any(state_diff > 0)


def is_state_progressive(state_diff: CellsArray, strict: bool = True) -> bool:
    return np.any(state_diff > 0) if strict else not np.any(state_diff < 0)
