from typing import Iterable

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer
import pytest
from bami.conflicts.conflict_set import IPv8ConflictSet, ConflictSetsMixin
from ipv8.test.mocking.ipv8 import MockIPv8

from tests.mocking.community import (
    FakeRoutines,
    MockSubCommunityRoutines,
    MockSubCommunityDiscoveryStrategy,
)
from tests.mocking.mock_db import MockPeerStore


class FakeConflictSets(ConflictSetsMixin, MockSubCommunityRoutines, FakeRoutines):
    def get_next_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass

    def get_prev_peers_for_request(self, state_id: bytes) -> Iterable[Peer]:
        pass

    @property
    def peer_store(self):
        return None


def test_is_sub(monkeypatch):
    monkeypatch.setattr(MockSubCommunityRoutines, "my_conflict_sets", [b"test1"])
    f = FakeConflictSets()
    assert f.is_subscribed(b"test1")


class TestSub:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.f = FakeConflictSets()

    def test_one_sub_when_subscribed(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_conflict_sets", [b"test1"])
        self.f.subscribe_to_conflict_set(b"test1")
        assert self.f.is_subscribed(b"test1")

    def test_no_ipv8(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_conflict_sets", [])
        monkeypatch.setattr(
            MockSubCommunityRoutines,
            "discovered_peers_by_conflict_set",
            lambda _, __: [],
        )
        f = FakeConflictSets()
        f.discovery_strategy = MockSubCommunityDiscoveryStrategy(None)
        f.subscribe_to_conflict_set(b"test1")

    def test_one_sub(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_conflict_sets", [])
        monkeypatch.setattr(
            MockSubCommunityRoutines,
            "discovered_peers_by_conflict_set",
            lambda _, __: [],
        )
        key = default_eccrypto.generate_key(u"medium").key_to_hash()
        monkeypatch.setattr(
            FakeRoutines,
            "ipv8",
            MockIPv8(
                u"curve25519", IPv8ConflictSet, cs_id=key, peer_store=MockPeerStore()
            ),
        )
        f = FakeConflictSets()
        f.discovery_strategy = MockSubCommunityDiscoveryStrategy(None)
        f.subscribe_to_conflict_set(b"test1")
