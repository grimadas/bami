# tests/conftest.py
import collections
from dataclasses import dataclass
from typing import List, Optional, Union

from _pytest.config import Config
from bami.backbone.payload import payload
from bami.backbone.transaction import Operation, OperationsManager, Transaction
from bami.backbone.utils import GEN_LINK
from bami.datastore.chain_store import BaseChain, Chain
from bami.datastore.database import BaseDB
from bami.sync.data_models import Link
from bami.sync.peer_state import CellIndexer
from ipv8.keyvault.crypto import default_eccrypto
import pytest

from tests.mocking.base import (
    create_and_connect_nodes,
    SetupValues,
    unload_nodes,
)


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "e2e: mark as end-to-end test.")


test_key = default_eccrypto.generate_key("curve25519")


@pytest.fixture
def common_test_key():
    return test_key


@payload
@dataclass
class MockOperation(Operation):
    value: int


def prepare_params(seed: bytes = b"0"):
    return [
        test_key.pub().key_to_bin(),
        b"test_state" * 2,
        [GEN_LINK],
        [MockOperation(b"test_key", [GEN_LINK], seed, 1)],
    ]


@pytest.fixture()
def test_params():
    OperationsManager.register_class(MockOperation)
    return prepare_params()


def create_block_batch(num_blocks=100, seed_val: bytes = b""):
    OperationsManager.register_class(MockOperation)
    blocks = []
    last_block_point = CellIndexer.create_cell_array()
    last_link = GEN_LINK
    for k in range(num_blocks):
        params = prepare_params(seed_val)
        params[2] = [last_link]
        tx = Transaction(*params)
        blocks.append(tx)
        last_block_point[CellIndexer.cell_id(hash(tx))] += 1
        last_link = Link(sum(last_block_point), tx.short_hash)
    return blocks


@pytest.fixture
def create_batches():
    def _create_batches(num_batches=2, num_blocks=100):
        """
        Creates batches of transactions within a random community.

        Args:
            num_batches: The number of batches to consider.
            num_blocks: The number of transactions in each batch.

        Returns: A list of batches where each batch represents a chain of transactions.

        """
        return [
            create_block_batch(num_blocks, i.to_bytes(4, "big"))
            for i in range(num_batches)
        ]

    return _create_batches


def insert_to_chain(chain_obj: BaseChain, blk: Transaction):
    block_links = blk.chain_links
    block_seq_num = blk.seq_num
    yield chain_obj.add_transaction(block_links, block_seq_num, blk.short_hash)


def insert_to_chain_or_blk_store(
    chain_obj: Union[BaseChain, BaseDB], blk: Transaction,
):
    if isinstance(chain_obj, BaseChain):
        yield from insert_to_chain(chain_obj, blk)
    else:
        yield chain_obj.add_transaction(blk, blk.pack())


def insert_batch_seq(
    chain_obj: Union[BaseChain, BaseDB], batch: List[Transaction],
) -> None:
    for blk in batch:
        yield from insert_to_chain_or_blk_store(chain_obj, blk)


def insert_batch_reversed(
    chain_obj: Union[BaseChain, BaseDB], batch: List[Transaction],
) -> None:
    for blk in reversed(batch):
        yield from insert_to_chain_or_blk_store(chain_obj, blk)


def insert_batch_random(
    chain_obj: Union[BaseChain, BaseDB], batch: List[Transaction],
) -> None:
    from random import shuffle

    shuffle(batch)
    for blk in batch:
        yield from insert_to_chain_or_blk_store(chain_obj, blk)


batch_insert_functions = [insert_batch_seq, insert_batch_random, insert_batch_reversed]


@pytest.fixture(params=batch_insert_functions)
def insert_function(request):
    param = request.param
    return param


@pytest.fixture
def chain():
    return Chain()


insert_function_copy = insert_function

_DirsNodes = collections.namedtuple("DirNodes", ("block_dirs", "peers_dirs", "nodes"))


def _set_vals_init(tmpdir_factory, overlay_class, num_nodes) -> _DirsNodes:
    block_dirs = [
        tmpdir_factory.mktemp(str(overlay_class.__name__) + "_block", numbered=True)
        for _ in range(num_nodes)
    ]
    peer_dirs = [
        tmpdir_factory.mktemp(str(overlay_class.__name__) + "_peers", numbered=True)
        for _ in range(num_nodes)
    ]

    nodes = create_and_connect_nodes(
        num_nodes,
        block_dirs=block_dirs,
        peer_store_dirs=peer_dirs,
        ov_class=overlay_class,
    )

    return _DirsNodes(block_dirs, peer_dirs, nodes)


def _set_vals_teardown(dirs) -> None:
    for k in dirs:
        k.remove(ignore_errors=True)


def _init_nodes(nodes, community_id: bytes) -> None:
    for node in nodes:
        node.overlay.subscribe_to_conflict_set(community_id)


@pytest.fixture
async def set_vals_by_key(
    tmpdir_factory,
    overlay_class,
    num_nodes: int,
    init_nodes: bool,
    community_id: Optional[bytes] = b"test_state" * 2,
):
    b_dirs, p_dirs, nodes = _set_vals_init(tmpdir_factory, overlay_class, num_nodes)
    # Make sure every node has a community to listen to
    if not community_id:
        community_key = default_eccrypto.generate_key("curve25519").pub()
        community_id = community_key.key_to_bin()
    if init_nodes:
        _init_nodes(nodes, community_id)
    yield SetupValues(nodes=nodes, community_id=community_id)
    await unload_nodes(nodes)
    _set_vals_teardown(b_dirs)
    _set_vals_teardown(p_dirs)


@pytest.fixture
async def set_vals_by_nodes(
    tmpdir_factory, overlay_class, num_nodes: int, init_nodes: bool
):
    b_dirs, p_dirs, nodes = _set_vals_init(tmpdir_factory, overlay_class, num_nodes)
    # Make sure every node has a community to listen to
    community_id = nodes[0].overlay.my_peer.mid
    if init_nodes:
        _init_nodes(nodes, community_id)
    yield SetupValues(nodes=nodes, community_id=community_id)
    await unload_nodes(nodes)
    _set_vals_teardown(b_dirs)
    _set_vals_teardown(p_dirs)
