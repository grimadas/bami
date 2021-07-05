from bami.backbone.transaction import Transaction
from bami.payment.community import PaymentCommunity
from bami.payment.processing_engine import (
    InvalidTransactionContextException,
    InvariantValidationException,
    PaymentState,
)
import pytest

from tests.mocking.base import deliver_messages


class FakePaymentCommunity(PaymentCommunity):
    pass


# Add tests for all exceptions
# Add tests on validity of transactions


@pytest.fixture(params=[5, 20])
def num_nodes(request):
    return request.param


@pytest.fixture
def overlay_class():
    return FakePaymentCommunity


@pytest.fixture
def init_nodes():
    return True


class TestInitCommunity:
    def test_empty(self, set_vals_by_nodes, num_nodes):
        nodes = set_vals_by_nodes.nodes
        assert len(nodes) == num_nodes

    def test_subscribe(self, set_vals_by_nodes):
        nodes = set_vals_by_nodes.nodes
        assert nodes[0].overlay.is_subscribed(set_vals_by_nodes.community_id)
        assert nodes[1].overlay.is_subscribed(set_vals_by_nodes.community_id)


class TestMint:
    @pytest.mark.asyncio
    async def test_valid_mint(self, set_vals_by_nodes):
        nodes = set_vals_by_nodes.nodes
        minter = nodes[0].overlay.my_peer.mid
        tx = nodes[0].overlay.mint(10)
        await deliver_messages(0.5)
        n_nodes = len(set_vals_by_nodes.nodes)
        for i in range(n_nodes):
            assert tx.hash in nodes[i].overlay.ee.processed_txs
            ps = nodes[i].overlay.get_balance(minter)
            assert ps.total_added - ps.total_subtracted > 0

    @pytest.mark.asyncio
    async def test_invalid_mint(self, set_vals_by_nodes):
        nodes = set_vals_by_nodes.nodes
        minter = nodes[0].overlay.my_peer.mid
        with pytest.raises(InvalidTransactionContextException):
            nodes[1].overlay.mint(10)


class TestSpend:
    @pytest.mark.asyncio
    async def test_invalid_spend(self, set_vals_by_nodes):
        vals = set_vals_by_nodes
        counter_party = vals.nodes[0].overlay.my_peer.mid
        with pytest.raises(InvariantValidationException):
            vals.nodes[1].overlay.transfer(
                state_id=vals.community_id, counter_party_id=counter_party, value=10,
            )

    def test_same_pack(self, set_vals_by_nodes):
        vals = set_vals_by_nodes
        tx1 = vals.nodes[0].overlay.mint(value=10)
        sh1 = tx1.operations[0].short_hash
        new_tx = Transaction.from_bytes(tx1.to_bytes())
        assert new_tx.operations[0].short_hash == sh1
        assert new_tx.operations[0] == tx1.operations[0]

    @pytest.mark.asyncio
    async def test_valid_spend(self, set_vals_by_nodes):
        vals = set_vals_by_nodes
        n_nodes = len(set_vals_by_nodes.nodes)
        minter = vals.nodes[0].overlay.my_peer.mid
        assert minter == vals.community_id
        assert vals.nodes[0].overlay.is_subscribed(minter)
        assert minter in vals.nodes[0].overlay.ee.known_minters

        tx1 = vals.nodes[0].overlay.mint(value=10)
        spender = minter
        counter_party = vals.nodes[1].overlay.my_peer.mid
        tx2 = vals.nodes[0].overlay.transfer(
            state_id=spender, counter_party_id=counter_party, value=10,
        )
        ps = vals.nodes[0].overlay.get_balance(spender)
        assert ps.total_added - ps.total_subtracted == 0
        assert tx1.hash in vals.nodes[0].overlay.ee.processed_txs
        assert tx2.hash in vals.nodes[0].overlay.ee.processed_txs

        await deliver_messages(0.5)
        # Should throw invalid mint exception
        for i in range(n_nodes):
            assert tx1.hash in vals.nodes[i].overlay.ee.processed_txs
            assert tx2.hash in vals.nodes[i].overlay.ee.processed_txs

            assert vals.nodes[i].overlay.get_balance(spender) == PaymentState(
                10, 10
            ), "Peer number {}".format(i)
            assert vals.nodes[i].overlay.get_balance(counter_party) == PaymentState(
                10, 0
            ), "Peer number {}".format(i)
