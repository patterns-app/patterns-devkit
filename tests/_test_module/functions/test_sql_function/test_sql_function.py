from __future__ import annotations

from snapflow import FunctionContext, SqlFunction


@SqlFunction(namespace="_test_module", file=__file__)
def test_sql_function():
    return "test_sql_function.sql"
