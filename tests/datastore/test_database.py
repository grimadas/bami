from typing import Iterator

from bami.backbone.transaction import Transaction
from bami.backbone.utils import (
    Dot,
    encode_raw,
    ShortKey,
    wrap_iterate,
)
from bami.datastore.database import DBManager, ProcessingType
from bami.sync.peer_state import CellIndexer
import pytest


class TestDBManager:
    @pytest.fixture(autouse=True)
    def setUp(self, tmpdir) -> None:
        tmp_val = tmpdir
        self.dbms = DBManager(str(tmp_val))
        yield
        self.dbms.close()
        tmp_val.remove()

    @pytest.fixture
    def std_vals(self, test_params):
        self.chain_id = b"state_id"
        self.block_dot = Dot((3, ShortKey("808080")))
        self.block_dot_encoded = encode_raw(self.block_dot)
        self.dot_id = self.chain_id + self.block_dot_encoded

        self.test_block = Transaction(*test_params)

        self.test_hash = hash(self.test_block)
        self.tx_blob = self.test_block.pack()
        self.state_id = list(self.test_block.state_id)[0]

        self.pers = self.test_block.creator_id
        self.res_cells = CellIndexer.create_cell_array()
        self.res_cells[CellIndexer.cell_id(self.test_hash)] += 1

    def test_add_transaction(self, std_vals):
        self.dbms.add_transaction(self.test_block, self.tx_blob)
        assert self.tx_blob == self.dbms.get_transaction_by_hash(self.test_hash)
        print(len(self.tx_blob))


class TestIntegrationDBManager:
    @pytest.fixture(autouse=True)
    def setUp(self, tmpdir) -> None:
        tmp_val = tmpdir
        self.dbms = DBManager(str(tmp_val), default_processing=ProcessingType.CHAIN)
        yield
        self.dbms.close()
        tmp_val.remove()

    @pytest.fixture(autouse=False)
    def setUp2(self, tmpdir) -> None:
        tmp_val = tmpdir
        self.dbms2 = DBManager(str(tmp_val))
        yield
        try:
            self.dbms2.close()
            tmp_val.remove()
        except FileNotFoundError:
            pass

    def test_add_notify_block(self, create_batches, insert_function):
        self.val_dots = []

        N = 10

        blks = create_batches(num_batches=1, num_blocks=N)
        com_id = blks[0][0].state_id

        def chain_dots_tester(order_tx: Iterator[Transaction]):
            for tx in order_tx:
                if len(self.val_dots) == 0:
                    assert tx.dot[0] == 1
                else:
                    assert tx.dot[0] > self.val_dots[-1][0]
                self.val_dots.append(tx.dot)

        self.dbms.order_tx.add_observer(com_id, chain_dots_tester)
        wrap_iterate(insert_function(self.dbms, blks[0]))
        assert len(self.val_dots) == N

    def test_add_notify_block_with_conflicts(self, create_batches, insert_function):
        self.val_dots = []

        def chain_dots_tester(dots):
            for tx in dots:
                self.val_dots.append(tx)

        blks = create_batches(num_batches=2, num_blocks=100)
        com_id = blks[0][0].state_id
        self.dbms.order_tx.add_observer(com_id, chain_dots_tester)

        wrap_iterate(insert_function(self.dbms, blks[0][:20]))
        wrap_iterate(insert_function(self.dbms, blks[1][20:60]))
        assert len(self.val_dots) == 20

        wrap_iterate(insert_function(self.dbms, blks[0][20:60]))
        assert len(self.val_dots) == 60

        wrap_iterate(insert_function(self.dbms, blks[1][0:20]))
        assert len(self.val_dots) == 120
