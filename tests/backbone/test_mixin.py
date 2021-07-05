from bami.backbone.mixins import StatedMixin
import pytest

from tests.mocking.base import create_node
from tests.mocking.community import FakeLightBackCommunity


class FakeStatedMixin(StatedMixin):
    def setup_mixin(self) -> None:
        self.inited = True

    def unload_mixin(self) -> None:
        self.unloaded = True


class MixinedBackCommunity(FakeLightBackCommunity, FakeStatedMixin):
    def __init__(self, *args, **kwargs):
        self.inited = False
        self.unloaded = False
        super().__init__(*args, **kwargs)


@pytest.mark.asyncio
async def test_mixin_logic(tmpdir_factory):
    dir = tmpdir_factory.mktemp(str(MixinedBackCommunity.__name__), numbered=True)
    node = create_node(MixinedBackCommunity, block_dir=str(dir))
    assert node.overlay.inited
    await node.stop()
    assert node.overlay.unloaded
    dir.remove(ignore_errors=True)
