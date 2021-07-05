from abc import ABC, abstractmethod
from typing import Iterator

from bami.backbone.transaction import Transaction


class CausalEngine(ABC):

    @abstractmethod
    def add_tx(self, tx: Transaction) -> Iterator:
        pass
