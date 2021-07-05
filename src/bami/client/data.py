from abc import ABCMeta, abstractmethod
from dataclasses import dataclass

# - Mint operation
from bami.backbone.payload import payload
from bami.backbone.transaction import Operation, register


# 3. Stateful validation:
#  - Validate with respect to the context (causal information)
# Data Context:
# - Previous operations that my transaction depends on
# - Client itself decides the context
# Execution context:
# - the transaction is executed according to the validation rules
# - the transaction is executed according to the execution policy
# - the conflict is resolved  via CRP heuristics
# Client doesn't need to specify the execution context (only optional)


@payload
@dataclass
class ArithmeticOperation(Operation, metaclass=ABCMeta):
    delta: int

    def is_valid(self) -> bool:
        return self.delta > 0

    @abstractmethod
    def __str__(self) -> str:
        pass

    def __hash__(self) -> int:
        return hash(repr(self))


@register
class AddOperation(ArithmeticOperation):
    def __str__(self) -> str:
        return "➕ op on {} with {}".format(self.key, self.delta)


@register
class SubtractOperation(ArithmeticOperation):
    def __str__(self) -> str:
        return "➖ op on {} with {}".format(self.key, self.delta)


class StatelessInvalidOperationException:
    pass
