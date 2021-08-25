from __future__ import annotations

from basis import sql_function


@sql_function(namespace="_test_module", file=__file__)
def test_sql_function():
    return "test_sql_function.sql"
