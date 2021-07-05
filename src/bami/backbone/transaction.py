from binascii import hexlify
from dataclasses import dataclass
from hashlib import sha256
import logging
import typing
from typing import List, Tuple, Type

from bami.backbone.payload import msg_payload, payload
from bami.backbone.utils import Dot, shorten
from bami.sync.data_models import Link
from ipv8.messaging.serialization import default_serializer, Serializable


class StatelessInvalidOperationException(Exception):
    pass


@payload
@dataclass
class Operation:
    key: bytes
    links: Tuple[Link]
    seed: bytes

    def is_valid(self) -> bool:
        return True

    def __post_init__(self):
        self.hash = self.calc_hash()
        self.short_hash = shorten(self.hash)
        self.seq_num = max(link.tx_index for link in self.links) + 1
        self.dot = Dot((self.seq_num, self.short_hash))
        self.ldot = Link(self.seq_num, self.short_hash)

    def calc_hash(self) -> bytes:
        return sha256(self.to_bytes()).digest()


class OperationsManager(object):
    op_id = 0
    op_classes = {}
    op_id_map = {}

    @classmethod
    def get_op_class(cls, bytes_id: bytes) -> Type[Operation]:
        return cls.op_id_map.get(bytes_id)

    @classmethod
    def get_op_id(cls, op_cls: Type[Operation]) -> bytes:
        return cls.op_classes.get(op_cls)

    @classmethod
    def register_class(cls, op: Type[Operation]) -> None:
        if op not in cls.op_classes:
            cls.op_id += 1
            cls.op_classes[op] = chr(cls.op_id).encode()
            cls.op_id_map[chr(cls.op_id).encode()] = op


def register(cls):
    OperationsManager.register_class(cls)
    return cls


@msg_payload
@dataclass
class BaseTransaction:
    """  Container for atomic execution of operations """

    creator_id: bytes
    state_id: bytes
    chain_links: List[Link]
    operations: List[Operation]

    def __post_init__(self):
        self.serializer = default_serializer
        self.hash = self.calculate_hash()
        self._logger = logging.getLogger(self.__class__.__name__)
        self.seq_num = self.calculate_seq_num()
        self.operating_keys = {o.key for o in self.operations}

    def calculate_hash(self) -> bytes:
        return sha256(self.pack()).digest()

    def calculate_seq_num(self) -> Tuple[int]:
        return max(link.tx_index for link in self.chain_links) + 1

    @property
    def dot(self) -> Dot:
        return Dot((self.seq_num, self.short_hash))

    @property
    def ldot(self) -> Dot:
        return Link(self.seq_num, self.short_hash)

    @property
    def hash_number(self):
        """Return the hash of this tx as a number (used as crawl ID). """
        return int(hexlify(self.hash), 16) % 100000000

    @property
    def short_pub_key(self):
        return int(hexlify(self.creator_id), 16) % 100000000

    @property
    def short_hash(self):
        return shorten(self.hash)

    def __hash__(self):
        return self.hash_number

    def __str__(self):
        return "🍱 Transaction {}/{} from {}: {}".format(
            self.hash_number, self.chain_links, self.creator_id, self.operations,
        )

    def pack(self) -> bytes:
        """
        Encode the tx
        Returns:
            Block bytes that can be signed
        """
        return self.to_bytes()

    @classmethod
    def unpack(cls, block_blob: bytes) -> "Transaction":
        return cls.from_bytes(block_blob)


class Transaction(BaseTransaction):
    def reformat(self):
        del self.names[-1]
        del self.format_list[-1]

        self.names.append("opcls")
        self.format_list.append("varlenH-list")

        self.names.append("opblobs")
        self.format_list.append("varlenH-list")

    def to_pack_list(self) -> typing.List[tuple]:
        out = []
        index = 0
        if "operations" in self.names:
            self.reformat()
        for i in range(len(self.format_list)):
            if self.names[index] == "opcls":
                args = []
                opcls = [
                    OperationsManager.get_op_id(type(op)) for op in self.operations
                ]
                args.append(opcls)
                out.append(("varlenH-list", *args))
                index += 1
            elif self.names[index] == "opblobs":
                args = []
                opblobs = [op.to_bytes() for op in self.operations]
                args.append(opblobs)
                out.append(("varlenH-list", *args))
                index += 1
            else:
                args = []
                for _ in range(8 if self.format_list[i] == "bits" else 1):
                    args.append(self._fix_pack(self.names[index]))
                    index += 1
                out.append((self._to_packlist_fmt(self.format_list[i]), *args))
        return out

    @classmethod
    def from_unpack_list(cls, *args) -> Serializable:
        unpack_args = list(args)
        for i in range(len(args)):
            custom_rule = "fix_unpack_" + cls.names[i]
            if hasattr(cls, custom_rule):
                unpack_args[i] = getattr(cls, custom_rule)(args[i])
        opcls = unpack_args[-2]
        opblobs = unpack_args[-1]
        ops = [
            OperationsManager.get_op_class(opcls[i]).from_bytes(opblobs[i])
            for i in range(len(opcls))
        ]
        del unpack_args[-2]
        del unpack_args[-1]
        unpack_args.append(ops)
        return cls(*unpack_args)
