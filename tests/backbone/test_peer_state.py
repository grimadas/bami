from bami.sync.peer_state import CellIndexer
import numpy as np


def test_cell_state_pack():
    cell_array = CellIndexer.create_cell_array()
    assert np.all(cell_array == 0)
    i = CellIndexer.cell_id(123)
    cell_array[i] += 1
    pc = CellIndexer.to_packable_cells(cell_array)
    assert pc.cell_additive == 0
    assert np.all(pc.cells == cell_array.tobytes())
