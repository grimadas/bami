from bami.backbone.transaction import Transaction
from bami.backbone.utils import GEN_LINK, GENESIS_HASH, shorten
from bami.client.data import AddOperation, SubtractOperation
from bami.payment.processing_engine import (
    InvalidOperationLinks,
    InvalidPermissionException,
    InvalidTransactionContextException,
    InvariantValidationException,
    PaymentProcessor,
    PaymentState,
)
from bami.sync.data_models import Link
import pytest


def test_hash_links():
    s = {Link(shorten(GENESIS_HASH), 0)}
    assert Link(shorten(GENESIS_HASH), 0) in s


def test_mint_and_transfer(state_processor: PaymentProcessor = None):
    if not state_processor:
        state_processor = PaymentProcessor()

    peer_a = b"test_peer_a"
    peer_b = b"test_peer_b"
    state_id = b"state_id"

    mint_op = AddOperation(peer_a, (GEN_LINK,), b"0", 100)
    spend_op = SubtractOperation(peer_a, (mint_op.ldot,), b"0", 10)
    claim_op = AddOperation(peer_b, (GEN_LINK,), b"0", 10)

    chain_links = (GEN_LINK,)

    tx = Transaction(peer_a, state_id, chain_links, (mint_op, spend_op, claim_op))

    state_processor.add_minter(peer_a)
    state_processor.process_transaction(tx)

    assert state_processor.get_last_state(peer_a) == PaymentState(100, 10)
    assert state_processor.get_last_state(peer_b) == PaymentState(10, 0)


def test_invalid_transfer():
    state_processor = PaymentProcessor()

    peer_a = b"test_peer_a"
    peer_b = b"test_peer_b"
    state_id = b"state_id"

    spend_op = SubtractOperation(peer_a, (GEN_LINK,), b"0", 10)
    claim_op = AddOperation(peer_b, (GEN_LINK,), b"0", 10)

    chain_links = (GEN_LINK,)

    tx = Transaction(peer_a, state_id, chain_links, (spend_op, claim_op))

    state_processor.add_minter(peer_a)

    with pytest.raises(InvariantValidationException):
        state_processor.process_transaction(tx)

    assert state_processor.get_last_state(peer_a) == PaymentState(0, 0)
    assert state_processor.get_last_state(peer_b) == PaymentState(0, 0)

    # Ensure peer can continue after the invalid transaction
    test_mint_and_transfer(state_processor)


def test_no_mint_permission():
    state_processor = PaymentProcessor()

    peer_a = b"test_peer_a"
    peer_b = b"test_peer_b"
    state_id = b"state_id"

    mint_op = AddOperation(peer_a, (GEN_LINK,), b"0", 100)
    spend_op = SubtractOperation(peer_a, (mint_op.ldot,), b"0", 10)
    claim_op = AddOperation(peer_b, (GEN_LINK,), b"0", 10)

    chain_links = (GEN_LINK,)

    tx = Transaction(peer_a, state_id, chain_links, (mint_op, spend_op, claim_op))

    with pytest.raises(InvalidTransactionContextException):
        state_processor.process_transaction(tx)

    assert state_processor.get_last_state(peer_a) == PaymentState(0, 0)
    assert state_processor.get_last_state(peer_b) == PaymentState(0, 0)

    test_mint_and_transfer(state_processor)


def test_no_modify_permissions():
    peer_a = b"test_peer_a"
    peer_b = b"test_peer_b"
    state_id = b"state_id"

    state_processor = PaymentProcessor()
    state_processor.has_all_permissions = (
        lambda x: False if x.creator_id == peer_a else True
    )

    mint_op = AddOperation(peer_a, (GEN_LINK,), b"0", 100)
    spend_op = SubtractOperation(peer_a, (mint_op.ldot,), b"0", 10)
    claim_op = AddOperation(peer_b, (GEN_LINK,), b"0", 10)

    chain_links = (GEN_LINK,)

    tx = Transaction(peer_a, state_id, chain_links, (mint_op, spend_op, claim_op))

    state_processor.add_minter(peer_a)

    with pytest.raises(InvalidPermissionException):
        state_processor.process_transaction(tx)

    assert state_processor.get_last_state(peer_a) == PaymentState(0, 0)
    assert state_processor.get_last_state(peer_b) == PaymentState(0, 0)

    state_processor.has_all_permissions = lambda x: True

    test_mint_and_transfer(state_processor)


def test_invalid_links():
    peer_a = b"test_peer_a"
    peer_b = b"test_peer_b"
    state_id = b"state_id"

    state_processor = PaymentProcessor()

    wrong_link = Link(5, b"0000001")

    mint_op = AddOperation(peer_a, (wrong_link,), b"0", 100)
    spend_op = SubtractOperation(peer_a, (mint_op.ldot,), b"0", 10)
    claim_op = AddOperation(peer_b, (GEN_LINK,), b"0", 10)

    chain_links = (GEN_LINK,)

    tx = Transaction(peer_a, state_id, chain_links, (mint_op, spend_op, claim_op))
    state_processor.add_minter(peer_a)

    with pytest.raises(InvalidOperationLinks):
        state_processor.process_transaction(tx)

    assert state_processor.get_last_state(peer_a) == PaymentState(0, 0)
    assert state_processor.get_last_state(peer_b) == PaymentState(0, 0)

    test_mint_and_transfer(state_processor)
