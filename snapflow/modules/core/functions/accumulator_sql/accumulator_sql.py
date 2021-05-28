from __future__ import annotations

from snapflow.core.data_block import SelfReference
from snapflow.core.execution import DataFunctionContext
from snapflow.core.function import Input, Output, datafunction
from snapflow.core.sql.sql_function import SqlDataFunctionWrapper, sql_datafunction
from snapflow.core.streams import Stream
from snapflow.utils.typing import T


@datafunction(namespace="core", display_name="Accumulate sql tables")
def accumulator_sql(
    ctx: DataFunctionContext,
    input: Stream[T],
    previous: SelfReference[T] = None,
) -> T:
    # TODO: right way: merge Schemas FIRST, then translate schema to column list (requires storage / dialect to quote)
    cols = []
    blocks = []
    if previous:
        blocks.append(previous)
    blocks.extend(input)
    select_stmts = []
    for block in blocks:
        cols += [c for c in block.realized_schema.field_names() if c not in cols]
    for block in blocks:
        col_sql = []
        for col in cols:
            if col in block.realized_schema.field_names():
                col_sql.append(col)
            else:
                col_sql.append("null as " + col)
        select_stmts.append(
            "select "
            + ",\n".join(col_sql)
            + " from "
            + block.as_sql_from_stmt(ctx.execution_config.get_target_storage())
        )
    sql = " union all ".join(select_stmts)
    sdf = SqlDataFunctionWrapper(sql)

    def noop(*args, **kwargs):
        return sql

    sdf.get_compiled_sql = noop
    return sdf(ctx, input=input, previous=previous)
