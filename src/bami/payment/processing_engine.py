from collections import defaultdict
import copy
from dataclasses import dataclass
import logging
from typing import Iterable, Tuple

from bami.backbone.transaction import Operation, Transaction
from bami.backbone.utils import Dot, GEN_DOT
from bami.client.data import AddOperation, SubtractOperation
from bami.datastore.chain_store import Chain
from bami.sync.data_models import Link


@dataclass(unsafe_hash=True)
class PaymentState:
    total_added: int
    total_subtracted: int

    def __str__(self) -> str:
        return "Balance is {}".format(self.total_added - self.total_subtracted)


class InvariantValidationException(Exception):
    pass


class InvalidPermissionException(Exception):
    pass


class InvalidTransactionContextException(Exception):
    pass


class InvalidOperationLinks(Exception):
    pass


class PaymentProcessor:
    def __init__(self) -> None:
        # Map of Key -> Chain object
        self.chains = defaultdict(lambda: Chain())
        self.committed_chains = defaultdict(lambda: Chain())

        self.last_consistent_dot = defaultdict(lambda: GEN_DOT)
        self.last_consistent_state = defaultdict(
            lambda: PaymentState(0, 0)
        )  # Last state: total added, total subtracted

        # Map of Key -> Dot -> State
        self.dot_states = defaultdict(lambda: {GEN_DOT: PaymentState(0, 0)})

        self.is_consistent = defaultdict(lambda: True)
        self.known_minters = set()

        self.op_tx_ind = {}
        self.processed_txs = set()

        self.logger = logging.getLogger("PaymentProcessor")

    def get_last_consistent_dots(self, key_id: bytes) -> Tuple[Link]:
        return list(
            Link(t[0], t[1]) for t in self.committed_chains[key_id].consistent_terminal
        )

    def get_last_const_tx(
        self, op_keys: Tuple[bytes], op_links: Tuple[Tuple[Link]]
    ) -> Tuple[Link]:
        tx_dots = set()
        for i in range(len(op_keys)):
            key = op_keys[i]
            for link in op_links[i]:
                if link.tx_index == 0:
                    tx_dots.add(GEN_DOT)
                else:
                    new_dot = self.op_tx_ind[(key, link)]
                    if new_dot == GEN_DOT:
                        raise Exception("%s, fef", new_dot)
                    tx_dots.add(new_dot)
        if GEN_DOT in tx_dots and len(tx_dots) > 1:
            tx_dots.remove(GEN_DOT)
        return [Link(*t) for t in tx_dots]

    @staticmethod
    def apply_update(old_state: PaymentState, op: Operation) -> PaymentState:
        new_state = PaymentState(old_state.total_added, old_state.total_subtracted)
        if type(op) == AddOperation:
            new_state.total_added += op.delta
        elif type(op) == SubtractOperation:
            new_state.total_subtracted += op.delta
        return new_state

    @staticmethod
    def check_invariants(state: PaymentState):
        return state.total_added - state.total_subtracted >= 0

    def ensure_chain_consistency(self, chain_id: bytes):
        if self.chains[chain_id].frontier != self.committed_chains[chain_id].frontier:
            self.chains[chain_id] = copy.deepcopy(self.committed_chains[chain_id])

    def _process_multi_link(
        self,
        lcs: PaymentState,
        prev_states: Iterable[PaymentState],
        prev_dots: Iterable[Dot],
    ):
        # Check previous state and choose or apply all, or cancel all
        all_added = sum((state.total_added - lcs.total_added) for state in prev_states)
        all_subtracted = sum(
            (state.total_subtracted - lcs.total_subtracted) for state in prev_states
        )

        # Deterministic conflict resolution:
        all_applicable = all_added - all_subtracted >= 0
        if not all_applicable:
            max_dot = max(prev_dots)
            old_state = self.dot_states[max_dot]
        else:
            old_state = PaymentState(
                all_added + lcs.total_added, all_subtracted + lcs.total_subtracted,
            )
        return old_state

    def _apply_tx(self, tx: Transaction):

        # Get old states of the transaction
        new_states = defaultdict(lambda: {})
        lcs = {}
        lcd = {}
        is_consistent = {}
        # Process
        for op in tx.operations:
            if op.key not in lcs:
                self.ensure_chain_consistency(op.key)
                is_consistent[op.key] = self.is_consistent[op.key]
                lcs[op.key] = self.last_consistent_state[op.key]
                lcd[op.key] = self.last_consistent_dot[op.key]

            res = self.chains[op.key].add_transaction(
                op.links, op.seq_num, op.short_hash
            )

            if not res:
                # Res should not be empty if links are valid
                raise InvalidOperationLinks(
                    "❌ Inconsistent or invalid links for operation ({}):{}".format(
                        op, op.links
                    )
                )

            for new_dot in res:
                try:
                    prev_dots = self.chains[op.key].get_prev_links(new_dot)
                    prev_states = [
                        self.dot_states[op.key][dot]
                        if dot in self.dot_states[op.key]
                        else new_states[op.key][dot]
                        for dot in prev_dots
                    ]
                except (TypeError, KeyError):
                    raise InvalidOperationLinks(
                        "❌ Inconsistent or invalid links for operation ({}):{}".format(
                            op, op.links
                        )
                    )
                if len(prev_states) > 1:
                    old_state = self._process_multi_link(
                        lcs[op.key], prev_states, prev_dots
                    )
                    is_consistent[op.key] = True
                elif is_consistent[op.key] and prev_dots[0] == lcd[op.key]:

                    old_state = (
                        self.dot_states[op.key][prev_dots[0]]
                        if prev_dots[0] in self.dot_states[op.key]
                        else new_states[op.key][prev_dots[0]]
                    )
                    lcd[op.key] = op.dot
                else:
                    old_state = (
                        self.dot_states[op.key][prev_dots[0]]
                        if prev_dots[0] in self.dot_states[op.key]
                        else new_states[op.key][prev_dots[0]]
                    )
                    # Revert to last consistent state
                    lcd[op.key] = prev_dots[0]
                    lcs[op.key] = self.dot_states[prev_dots[0]]
                    is_consistent[op.key] = False

                new_state = self.apply_update(old_state, op)
                if not self.check_invariants(new_state):
                    raise InvariantValidationException(
                        "❌ Transaction {} cannot be applied! (Operation {}) breaks invariants".format(
                            tx, op
                        )
                    )
                if is_consistent[op.key]:
                    lcs[op.key] = new_state
                new_states[op.key][op.dot] = new_state

        # Commit
        for k, v in lcs.items():
            self.last_consistent_state[k] = v
        for k, v in lcd.items():
            self.last_consistent_dot[k] = v
        for k, dot_state in new_states.items():
            for dot, state in dot_state.items():
                self.dot_states[k][dot] = state
        for op in tx.operations:
            self.committed_chains[op.key].add_transaction(
                op.links, op.seq_num, op.short_hash
            )
            self.op_tx_ind[(op.key, op.ldot)] = tx.dot

    def allowed_to_mint(self, creator: bytes):
        return creator in self.known_minters

    def add_minter(self, creator: bytes):
        self.logger.info("🪙 Adding a Minter %s", creator)
        self.known_minters.add(creator)

    def _process_tx_context(
        self,
        creator: bytes,
        other_add: int,
        self_subtract: int,
        other_subtract: int,
        self_add: int,
    ) -> None:
        """
        @raise InvalidTransactionContextException
        """
        # Peer is allowed to add to others if this is transfer or valid mint
        if (
            other_add > 0
            and not self.allowed_to_mint(creator)
            and other_add > self_subtract
        ):
            raise InvalidTransactionContextException(
                "❌ %s cannot add/create tokens", creator
            )
        # Peer is not allowed to subtract other wallets
        if other_subtract:
            raise InvalidTransactionContextException(
                "❌ {} is not allowed to subtract other accounts".format(creator)
            )
        # Peer is allowed to add to own key only if minter
        if self_add > 0 and not self.allowed_to_mint(creator):
            raise InvalidTransactionContextException("❌ {} cannot mint".format(creator))

    def has_all_permissions(self, tx: Transaction):
        # TODO: implement if needed
        return True

    def get_last_state(self, key_id: bytes) -> PaymentState:
        return self.last_consistent_state[key_id]

    def process_transaction(self, tx: Transaction) -> None:
        """
        @raise InvalidPermissionException: if creator has no permissions to modify the keys
        @raise InvalidTransactionContextException: if transaction context is not valid
        @raise InvariantValidationException: if transaction breaks invariants
        """
        if tx.hash in self.processed_txs:
            return

        # 1. Check if creator has all permissions for the transaction
        if not self.has_all_permissions(tx):
            raise InvalidPermissionException(
                "❌ {} has no permissions for {}", tx.creator_id, tx.operating_keys
            )
        # 2. Check if the transaction context is valid
        #  a. Build transaction context
        creator = tx.creator_id
        add_changes = defaultdict(lambda: 0)
        subtract_changes = defaultdict(lambda: 0)
        key_links = {}

        for o in tx.operations:
            if type(o) == AddOperation:
                add_changes[o.key] += o.delta
            elif type(o) == SubtractOperation:
                subtract_changes[o.key] += o.delta
            key_links[o.key] = o.links

        # b. Get payment values
        self_subtract = subtract_changes[creator]
        other_add = sum(v for k, v in add_changes.items() if k != creator)
        other_subtract = sum(v for k, v in subtract_changes.items() if k != creator)
        self_add = add_changes[creator]

        # c. Check if the operations are valid in the context of a transaction
        self._process_tx_context(
            creator, other_add, self_subtract, other_subtract, self_add
        )

        # 3. Apply transaction
        self._apply_tx(tx)

        self.processed_txs.add(tx.hash)

    def __str__(self) -> str:
        return "💰 PaymentProcessor"
