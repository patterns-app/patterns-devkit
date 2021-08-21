from __future__ import annotations

from basis import Context, datafunction


@datafunction(namespace="_test_module")
def test_function(ctx: Context):
    pass
