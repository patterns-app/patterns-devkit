from __future__ import annotations

from snapflow.core.sql.sql_function import SqlFunction


@SqlFunction(
    namespace="core",
    autodetect_inputs=False,
    display_name="Accumulate Tables",
    file=__file__,
)
def accumulator_sql():
    return "accumulator_sql.sql"
