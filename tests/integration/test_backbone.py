from bami.backbone.community import BamiCommunity
from bami.backbone.transaction import Transaction
from bami.conflicts.conflict_set import LightConflictSetFactory
import pytest

from tests.mocking.base import deliver_messages


class SimpleCommunity(BamiCommunity, LightConflictSetFactory):
    """
    A very basic community with no additional functionality. Used during the integration tests.
    """

    pass


@pytest.fixture
def init_nodes():
    return True


@pytest.fixture
def community_id():
    return b"test_state"


@pytest.mark.parametrize("overlay_class", [SimpleCommunity])
@pytest.mark.parametrize("num_nodes", [2])
def test_is_subscribed(test_params, set_vals_by_key):
    """
    Test whether missing transactions are synchronized after a network partition.
    """
    vals = set_vals_by_key
    com_id = vals.community_id
    v = len(vals.nodes)

    for i in range(v):
        set_vals_by_key.nodes[0].overlay.is_subscribed(com_id)
        set_vals_by_key.nodes[1].overlay.is_subscribed(com_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("overlay_class", [SimpleCommunity])
@pytest.mark.parametrize("num_nodes", [2])
async def test_simple_frontier_reconciliation_after_partition(
    set_vals_by_key, test_params, overlay_class, num_nodes
):
    """
    Test whether missing transactions are synchronized after a network partition.
    """
    for _ in range(1):
        # Note that we do not broadcast the tx to the other node
        params = test_params
        # Create and push transactions
        tx = Transaction(*params)
        set_vals_by_key.nodes[0].overlay.create_and_store_tx(tx)

    # Force frontier exchange
    await deliver_messages(timeout=0.6)

    frontier1 = (
        set_vals_by_key.nodes[0]
        .overlay.persistence.get_engine(set_vals_by_key.community_id)
        .frontier
    )
    frontier2 = (
        set_vals_by_key.nodes[1]
        .overlay.persistence.get_engine(set_vals_by_key.community_id)
        .frontier
    )
    assert len(frontier2.terminal) == 1
    assert frontier2.terminal[0][0] == 1
    assert frontier1 == frontier2
