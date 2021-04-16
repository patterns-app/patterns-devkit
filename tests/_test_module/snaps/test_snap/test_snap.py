from __future__ import annotations
from snapflow import Snap, SnapContext


@Snap(namespace="_test_module")
def test_snap(ctx: SnapContext):
    pass
