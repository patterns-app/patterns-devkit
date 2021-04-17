from __future__ import annotations
from snapflow import Function, FunctionContext


@Function(namespace="_test_module")
def test_function(ctx: FunctionContext):
    pass
