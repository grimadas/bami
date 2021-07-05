from __future__ import annotations

from abc import ABCMeta
from decimal import Decimal
import random
from typing import Iterator, Set, Tuple, Type

from bami.backbone.community import BamiCommunity
from bami.backbone.transaction import Operation, Transaction
from bami.client.data import AddOperation, ArithmeticOperation, SubtractOperation
from bami.conflicts.conflict_set import IPv8ConflictSetFactory
from bami.payment.processing_engine import (
    InvalidOperationLinks,
    InvalidPermissionException,
    InvalidTransactionContextException,
    InvariantValidationException,
    PaymentProcessor,
    PaymentState,
)


class BasePaymentCommunity(BamiCommunity, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ee = PaymentProcessor()

    def on_settled_transactions(self, tx_ids: Set[int]) -> None:
        super().on_settled_transactions(tx_ids)

    def on_join_cs(self, cs_id: bytes) -> None:
        super().on_join_cs(cs_id)

        self.ee.add_minter(cs_id)
        self.subscribe_on_ordered_updates(cs_id, self.process_transactions)

    def get_balance(self, key: bytes) -> PaymentState:
        return self.ee.last_consistent_state[key]

    def react_on_invalid_transaction(self, tx: Transaction):
        # TODO: add reaction to an invalid transaction
        pass

    def process_transactions(self, txs: Iterator[Transaction]) -> None:
        self.logger.debug("Start Processing transaction %s", txs)
        for tx in txs:
            try:
                self.logger.debug("Processing transaction %s", tx)
                self.ee.process_transaction(tx)
            except (
                InvalidOperationLinks,
                InvariantValidationException,
                InvalidTransactionContextException,
                InvalidPermissionException,
            ) as e:
                self.logger.error("Invalid tx %s with exception %s", tx, e)
                self.react_on_invalid_transaction(tx)

    def prepare_operation(
        self, key: bytes, value: int, Operation: Type[ArithmeticOperation]
    ) -> ArithmeticOperation:
        seed = random.getrandbits(8).to_bytes(1, "big")
        last_cons_dot = self.ee.get_last_consistent_dots(key)
        return Operation(key, last_cons_dot, seed, value)

    def prepare_transaction(self, ops: Tuple[Operation], state_id: bytes):

        op_keys = [op.key for op in ops]
        op_dots = [op.links for op in ops]
        chain_links = self.ee.get_last_const_tx(op_keys, op_dots)
        self.logger.info(
            "Creating transaction with %s, %s, %s, %s",
            self.my_peer.mid,
            state_id,
            chain_links,
            ops,
        )
        return Transaction(self.my_peer.mid, state_id, chain_links, ops)

    def mint(
        self, value: int, to_peer: bytes = None, state_id: bytes = None
    ) -> Transaction:
        if not state_id:
            # Community id is the same as the peer id
            state_id = self.my_peer.mid
        if not to_peer:
            to_peer = self.my_peer.mid

        mint_operation = self.prepare_operation(to_peer, value, AddOperation)
        tx = self.prepare_transaction((mint_operation,), state_id)
        self.logger.info("Mint transaction created %s", tx)
        # Verify transaction first - will throw exception
        self.ee.process_transaction(tx)
        tx_pack = self.create_and_store_tx(tx)
        self.share_in_community(tx_pack, state_id)
        return tx

    def transfer(
        self, state_id: bytes, counter_party_id: bytes, value: int
    ) -> Transaction:

        my_key = self.my_peer.mid

        s1 = self.prepare_operation(my_key, value, SubtractOperation)
        s2 = self.prepare_operation(counter_party_id, value, AddOperation)
        tx = self.prepare_transaction((s1, s2), state_id)
        self.logger.info("Transfer transaction created %s", tx)
        # Verify transaction first - will throw exception
        self.ee.process_transaction(tx)
        tx_pack = self.create_and_store_tx(tx)
        self.share_in_community(tx_pack, state_id)
        return tx


class PaymentCommunity(BasePaymentCommunity, IPv8ConflictSetFactory):
    pass
