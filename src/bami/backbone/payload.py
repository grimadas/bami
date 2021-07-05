from dataclasses import _get_field
import typing
from typing import Type

from ipv8.messaging.lazy_payload import VariablePayload
from ipv8.messaging.serialization import default_serializer, Serializable


def get_cls_fields(cls):
    cls_annotations = cls.__dict__.get("__annotations__", {})

    # Now find fields in our class.  While doing so, validate some
    # things, and set the default values (as class attributes) where
    # we can.
    cls_fields = [_get_field(cls, name, type) for name, type in cls_annotations.items()]
    return {f.name: f.type for f in cls_fields}


MAX_PACKET_SIZE = 1400


def check_size_limit(pack):
    if len(pack) > MAX_PACKET_SIZE:
        return False
    return True


class PayloadsManager(object):
    msg_id = 1
    payloads = {}

    @classmethod
    def enrich(cls, payload_cls: Type[Serializable]) -> Type[Serializable]:
        name = payload_cls.__name__
        if name not in cls.payloads:
            cls.msg_id += 1
            cls.payloads[name] = cls.msg_id
        payload_cls.msg_id = cls.payloads[name]
        return payload_cls


class ImpSer(Serializable):
    def to_pack_list(self) -> typing.List[tuple]:
        pass

    @classmethod
    def from_unpack_list(cls, *args) -> Serializable:
        pass


class BamiPayload:
    serializer = default_serializer

    @staticmethod
    def _type_map(t: Type) -> str:
        if t == int:
            return "Q"
        elif t == bytes:
            return "varlenH"
        elif "Tuple" in str(t) or "List" in str(t) or "Set" in str(t):
            return (
                "varlenH-list"
                if "int" in str(t) or "bytes" in str(t)
                else [typing.get_args(t)[0]]
            )
        elif hasattr(t, "format_list"):
            return t
        else:
            raise NotImplementedError(t, " unknown")

    @classmethod
    def init_class(cls):
        # Copy all methods of VariablePayload except init
        d = {
            k: v
            for k, v in VariablePayload.__dict__.items()
            if not str(k).startswith("__")
            and str(k) != "names"
            and str(k) != "format_list"
        }

        for (name, method) in d.items():
            setattr(cls, name, method)
        # Populate names and format list
        fields = get_cls_fields(cls)

        for f, t in fields.items():
            cls.names.append(f)
            cls.format_list.append(cls._type_map(t))
        return cls

    def is_optimal_size(self):
        return check_size_limit(self.to_bytes())

    def to_bytes(self) -> bytes:
        return self.serializer.pack_serializable(self)

    @classmethod
    def from_bytes(cls, pack: bytes) -> "BamiPayload":
        return cls.serializer.unpack_serializable(cls, pack)[0]


def payload(cls):
    d = {k: v for k, v in BamiPayload.__dict__.items() if not str(k).startswith("__")}
    for k, v in d.items():
        setattr(cls, k, v)
    cls.names = list()
    cls.format_list = list()

    # Populate all by mro
    added_classes = set()
    new_mro = []
    has_imp_ser = False

    for superclass in cls.__mro__:
        if superclass == ImpSer:
            has_imp_ser = True
        if hasattr(superclass, "names"):
            cls.names.extend(superclass.names)
            cls.format_list.extend(superclass.format_list)

    new_mro.append(ImpSer)
    if ImpSer not in added_classes:
        added_classes.add(ImpSer)

    if not has_imp_ser:
        new_vals = tuple([ImpSer] + list(cls.__bases__))
        new_cls = type(cls.__name__, new_vals, dict(cls.__dict__))
    else:
        new_cls = cls

    return new_cls.init_class()


def msg_payload(cls):
    return PayloadsManager.enrich(payload(cls))


def enrich(cls):
    return PayloadsManager.enrich(cls)


class MaxPacketException(Exception):
    pass
