from dataclasses import dataclass

from bami.backbone.bloom_filter import (
    bitarray_to_bytes,
    BloomFilter,
    BloomFiltersManager,
    both_present,
    bytes_to_bitarray,
    filter_id,
    iterate_bloom_filter,
)
from bami.sync.peer_state import CellIndexer
import numpy as np
import pytest

from tests.conftest import create_block_batch


@pytest.mark.parametrize("bits", [[0] * 8, [1] * 8, [0, 1] * 4, [1, 0] * 4])
def test_convert_bits_to_bytes(bits):
    assert np.array_equal(bytes_to_bitarray(bitarray_to_bytes(bits)), bits)


@pytest.mark.parametrize(
    "b1,b2,len_res",
    [
        (np.array([0, 1, 0, 1]), np.array([0, 1, 0, 1]), 2),
        (np.array([1, 0, 0, 1]), np.array([0, 1, 1, 0]), 0),
    ],
)
def test_both_present(b1, b2, len_res):
    assert len(both_present(b1, b2)) == len_res


def test_bloom_filter():
    m = 8
    v = 5556

    b = BloomFilter(m)
    assert not b.maybe_element(v)
    b.add(v)
    assert b.maybe_element(v)

    prev_hash_func = b.hash_funcs[0]
    b.reindex(new_seed_value=1)
    b.add(v)

    assert b.maybe_element(v)
    assert b.hash_funcs[0] != prev_hash_func


def test_bloom_manager():
    state_id = b"state_id"
    cp_id = b"01010ffb"

    bf = BloomFiltersManager.bloom_filter(state_id, cp_id)
    assert bf.seed_value == BloomFiltersManager.get_init_seed(cp_id)

    bf2 = BloomFiltersManager.bloom_filter(state_id, cp_id)
    assert bf == bf2


@dataclass
class ExpValues:
    state_id = b"state_id"
    pa_id = b"ff01"
    pb_id = b"aa01"
    f_id = filter_id(state_id, pb_id)


@pytest.mark.parametrize(
    "pa_prev_tx,pa_cur_tx,pb_prev_tx,pb_cur_tx",
    [(3, 6, 2, 5), (3, 6, 0, 6), (3, 6, 3, 1),],
)
def test_iterate_bloom_filter(pa_prev_tx, pa_cur_tx, pb_prev_tx, pb_cur_tx):
    # 1. Initialize bloom filters of peer A and peer B
    BloomFiltersManager.init_manager(ExpValues.pa_id)
    pb_bf = BloomFilter(
        BloomFiltersManager.size, BloomFiltersManager.get_init_seed(ExpValues.pb_id)
    )

    # 2. First round: fill txs_hashes
    i_prev = max(pa_prev_tx, pb_prev_tx)
    txs = create_block_batch(i_prev)
    # populate bloom filters
    peer_b_cells = CellIndexer.create_cell_array()
    for i in range(i_prev):
        t = hash(txs[i])
        if i < pa_prev_tx:
            BloomFiltersManager.add_to_bloom_index(
                ExpValues.state_id, ExpValues.pb_id, t
            )
        if i < pb_prev_tx:
            pb_bf.add(t)
            peer_b_cells[CellIndexer.cell_id(t)] += 1

    # ... 3. Peer a receives bloom filter
    last_peer_bf = pb_bf.to_filter_pack()

    # 4. Second round
    i_prev = max(pa_cur_tx, pb_cur_tx)
    txs = create_block_batch(i_prev)

    peer_b_cells_1 = np.copy(peer_b_cells)

    # populate bloom filters
    for i in range(i_prev):
        t = hash(txs[i])
        if i < pa_cur_tx:
            BloomFiltersManager.add_to_bloom_index(
                ExpValues.state_id, ExpValues.pb_id, t
            )
        if i < pb_cur_tx:
            pb_bf.add(t)
            peer_b_cells[CellIndexer.cell_id(t)] += 1
    # 5. Peer A receives new bloom filter and triggers iter bloom filter
    cell_diff = peer_b_cells_1

    new_txs, settled_txs = iterate_bloom_filter(
        cell_diff, ExpValues.state_id, ExpValues.pb_id, last_peer_bf
    )
    assert len(settled_txs) == pb_prev_tx

    assert len(new_txs) == pa_cur_tx - pb_prev_tx
