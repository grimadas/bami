from bami.backbone.transaction import Operation, Transaction
from bami.sync.data_models import Link

from tests.conftest import MockOperation


def test_pack_operation(test_params):
    op1 = Operation(b"1", [Link(1, b"232")], b"12")
    assert Operation.from_bytes(op1.to_bytes())


def test_pack_test_operation(test_params):
    op1 = MockOperation(b"1", [Link(1, b"232")], b"12", 1)
    assert MockOperation.from_bytes(op1.to_bytes())


def test_pack_unpack_transaction(test_params, common_test_key):
    t1 = Transaction(*test_params)
    v = t1.to_bytes()
    assert t1 == Transaction.from_bytes(v)
