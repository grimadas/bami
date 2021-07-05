from dataclasses import dataclass
from typing import List

from bami.backbone.payload import payload


@payload
@dataclass
class Links:
    index: int
    hash: bytes


@payload
@dataclass
class NewLinks(Links):
    varq: bytes


@payload
@dataclass
class VarLe:
    varq: List[bytes]


@payload
@dataclass
class NestedLinks:
    v1: int
    v3: NewLinks


@payload
@dataclass
class ComplexData:
    v1: int
    v2: List[bytes]
    v3: NewLinks
    v4: List[Links]


def test_op_pack(test_params):
    l = Links(1, b"123123")
    assert l.names == ["index", "hash"]
    v = Links.from_bytes(l.to_bytes())
    assert l == v
    l2 = Links(2, b"21312")
    assert l2 == Links.from_bytes(l2.to_bytes())


def test_inherited_payloads():
    l = NewLinks(1, b"123123", b"val")
    assert l.names == ["index", "hash", "varq"]
    assert l == NewLinks.from_bytes(l.to_bytes())


def test_varq():
    l = VarLe([chr(1).encode()])
    assert l == VarLe.from_bytes(l.to_bytes())


def test_convert_nested_data():
    cd = NestedLinks(1, NewLinks(1, b"2323", b"123"))
    assert cd == NestedLinks.from_bytes(cd.to_bytes())


def test_convert_complex_payload():
    cd = ComplexData(
        1,
        [b"11", b"2323"],
        NewLinks(1, b"2323", b"123"),
        [Links(1, b"2323"), Links(2, b"23232")],
    )
    assert cd == ComplexData.from_bytes(cd.to_bytes())
    print(cd)
