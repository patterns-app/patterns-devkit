from __future__ import annotations

from basis import FunctionContext, datafunction


@datafunction(namespace="_test_module")
def test_function(ctx: FunctionContext):
    pass
