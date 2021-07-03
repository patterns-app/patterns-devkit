from __future__ import annotations

from basis import DataFunctionContext, datafunction


@datafunction(namespace="_test_module")
def test_function(ctx: DataFunctionContext):
    pass
