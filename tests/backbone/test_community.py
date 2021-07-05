from bami.backbone.transaction import Transaction
import pytest

from tests.mocking.base import (
    deliver_messages,
    introduce_nodes,
)
from tests.mocking.community import (
    FakeIPv8BackCommunity,
    FakeLightBackCommunity,
)

NUM_NODES = 2


@pytest.fixture(params=[FakeIPv8BackCommunity, FakeLightBackCommunity])
def overlay_class(request):
    return request.param


@pytest.fixture
def init_nodes():
    return True


@pytest.fixture
def num_nodes():
    return NUM_NODES


@pytest.mark.asyncio
async def test_share_in_community(mocker, set_vals_by_key, test_params):
    blk = Transaction(*test_params)

    set_vals_by_key.nodes[0].overlay.share_in_community(
        blk, set_vals_by_key.community_id
    )
    spy = mocker.spy(set_vals_by_key.nodes[1].overlay, "validate_persist_transaction")
    await deliver_messages()
    spy.assert_called()


def test_subscribe(set_vals_by_key):
    assert set_vals_by_key.nodes[0].overlay.is_subscribed(set_vals_by_key.community_id)
    assert set_vals_by_key.nodes[1].overlay.is_subscribed(set_vals_by_key.community_id)


@pytest.mark.asyncio
async def test_peers_introduction(mocker, set_vals_by_key):
    spy = mocker.spy(set_vals_by_key.nodes[1].overlay, "process_peer_subscriptions")
    await introduce_nodes(set_vals_by_key.nodes)
    spy.assert_called()
    for i in range(NUM_NODES):
        assert len(set_vals_by_key.nodes[i].overlay.my_conflict_sets) == 1
        assert (
            set_vals_by_key.nodes[i].overlay.get_conflict_set(
                set_vals_by_key.community_id
            )
            is not None
        )
        assert (
            len(
                set_vals_by_key.nodes[i]
                .overlay.get_conflict_set(set_vals_by_key.community_id)
                .get_known_peers()
            )
            > 0
        )


# TODO: Test subscribe multiple communities
