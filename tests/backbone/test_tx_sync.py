from typing import Iterable
from unittest.mock import ANY

from bami.backbone.transaction import Transaction
from bami.backbone.transaction_sync import TransactionSyncMixin
from bami.datastore.peer_store import BasePeerStore
from ipv8.peer import Peer
import pytest

from tests.mocking.base import deliver_messages
from tests.mocking.community import MockedCommunity
from tests.mocking.mock_db import MockDBManager, MockPeerStore


class TransactionSyncCommunity(MockedCommunity, TransactionSyncMixin):
    @property
    def peer_store(self) -> BasePeerStore:
        return MockPeerStore()

    def get_next_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass

    def get_prev_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass


@pytest.fixture()
def overlay_class():
    return TransactionSyncCommunity


@pytest.fixture()
def init_nodes():
    return False


@pytest.fixture()
def num_nodes():
    return 2


@pytest.mark.asyncio
async def test_send_receive_transaction(
    monkeypatch, mocker, set_vals_by_key, test_params
):
    monkeypatch.setattr(MockDBManager, "has_transaction", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_transaction")

    blk = Transaction(*test_params)
    set_vals_by_key.nodes[0].overlay.send_payload(
        set_vals_by_key.nodes[1].overlay.my_peer, blk
    )
    await deliver_messages()
    spy.assert_called_with(ANY, hash(blk))


@pytest.mark.asyncio
async def test_send_receive_raw_transaction(
    monkeypatch, mocker, set_vals_by_key, test_params
):
    monkeypatch.setattr(MockDBManager, "has_transaction", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_transaction")
    blk = Transaction(*test_params)
    blk_packet = set_vals_by_key.nodes[0].overlay.prepare_packet(blk)
    set_vals_by_key.nodes[0].overlay.send_packet(
        set_vals_by_key.nodes[1].overlay.my_peer.address, blk_packet
    )
    await deliver_messages()
    spy.assert_called_with(ANY, hash(blk))


def test_create_apply_transaction(monkeypatch, mocker, set_vals_by_key, test_params):
    monkeypatch.setattr(MockDBManager, "has_transaction", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_transaction")
    blk = Transaction(*test_params)

    set_vals_by_key.nodes[0].overlay.create_and_store_tx(blk)
    spy.assert_called_with(ANY, hash(blk))


@pytest.mark.asyncio
async def test_send_incorrect_transaction(
    monkeypatch, mocker, set_vals_by_key, test_params
):
    monkeypatch.setattr(MockDBManager, "has_transaction", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_transaction")
    spy2 = mocker.spy(set_vals_by_key.nodes[1].overlay, "received_transaction")

    blk = Transaction(*test_params)
    set_vals_by_key.nodes[0].overlay.send_packet(
        set_vals_by_key.nodes[1].overlay.my_peer.address, blk.pack()
    )
    await deliver_messages()
    spy.assert_not_called()
    spy2.assert_not_called()
