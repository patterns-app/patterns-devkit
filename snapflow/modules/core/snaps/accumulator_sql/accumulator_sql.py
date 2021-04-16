from __future__ import annotations

from snapflow.core.sql.sql_snap import SqlSnap
from snapflow.core.snap import Input, Output, Snap


@Input("previous", schema="T", from_self=True)
@Input("new", schema="T", stream=True)
@Output(schema="T")
@SqlSnap(
    namespace="core",
    autodetect_inputs=False,
    display_name="Accumulate Tables",
    file=__file__,
)
def accumulator_sql():
    return "accumulator_sql.sql"