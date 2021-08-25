from __future__ import annotations

from basis import sqlfunction


@sqlfunction(file=__file__)
def test_sql_function():
    return "test_sql_function.sql"
