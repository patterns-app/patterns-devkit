from __future__ import annotations

from snapflow.core.sql.sql_function import SqlFunction
from snapflow.core.function import Input, Output, Function


@Input("previous", schema="T", from_self=True)
@Input("new", schema="T", stream=True)
@Output(schema="T")
@SqlFunction(
    namespace="core",
    autodetect_inputs=False,
    display_name="Accumulate Tables",
    file=__file__,
)
def accumulator_sql():
    return "accumulator_sql.sql"
