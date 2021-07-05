from dataclasses import dataclass
from heapq import heappop, heappush
from typing import Iterator, Tuple

from bami.backbone.transaction import Transaction
from bami.datastore.causality_engine import CausalEngine
from bami.sync.peer_state import CellIndexer
import numpy as np


@dataclass
class CausalIndex:
    causal_index: Tuple[int]
    tx: Transaction

    def __lt__(self, other: "CausalIndex"):
        return np.all(self.causal_index <= other.causal_index)


def is_consistent_with_local_data(my_link: Tuple[int], tx_link: Tuple[int]) -> bool:
    return np.all(my_link >= tx_link)


class Heap(CausalEngine):
    def __init__(self) -> None:
        self.frontiers = []
        self.cells = CellIndexer.create_cell_array()

    def add_tx(self, tx: Transaction) -> Iterator:
        ca = tuple(CellIndexer.to_cells_array(tx.creator_state[state_index]))
        heappush(self.frontiers, CausalIndex(ca, tx))
        if not is_consistent_with_local_data(self.cells, ca):
            return []
        else:
            return list(self.get_tx_with_order())

    def __str__(self) -> str:
        return "📚 Heap: pending tx: {}, total: {}".format(
            len(self.frontiers), np.sum(self.cells)
        )

    def update_cell_state(self, tx_hash: int):
        self.cells[CellIndexer.cell_id(tx_hash)] += 1

    def get_tx_with_order(self) -> Iterator[Transaction]:
        while self.frontiers:
            ci: CausalIndex = heappop(self.frontiers)
            my_state = self.cells
            if is_consistent_with_local_data(my_state, ci.causal_index):
                self.update_cell_state(hash(ci.tx))
                yield ci.tx
            else:
                heappush(self.frontiers, ci)
                break
