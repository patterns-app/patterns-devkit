from __future__ import annotations

from typing import Optional

from commonmodel.base import Schema
from dcp.utils.common import T
from snapflow import DataBlock, DataFunctionContext, datafunction
from snapflow.core.sql.sql_function import SqlDataFunctionWrapper


@datafunction(
    namespace="core",
    display_name="Dedupe Table (keep latest)",
    required_storage_engines=["postgresql"],
    # TODO: requires postgres StorageEngine
)
def dedupe_keep_latest_sql(
    ctx: DataFunctionContext, input: DataBlock[T]
) -> DataBlock[T]:
    # return "dedupe_keep_latest_sql.sql"
    nominal: Optional[Schema] = None
    realized: Schema = ctx.library.get_schema(input.realized_schema_key)
    distinct_on_cols = []
    target_storage = ctx.execution_config.get_target_storage()
    as_identifier = target_storage.get_api().get_quoted_identifier

    def identifiers(i):
        return [as_identifier(s) for s in i]

    if input.nominal_schema_key:
        nominal = ctx.library.get_schema(input.nominal_schema_key)
        if nominal.unique_on:
            distinct_on_cols = nominal.unique_on
    if not distinct_on_cols:
        distinct_on_cols = realized.field_names()
    distinct_clause = ""
    if distinct_on_cols:
        distinct_clause = f" distinct on ({', '.join(identifiers(distinct_on_cols))})"
    cols = ", ".join(identifiers(realized.field_names()))

    orderby_clause = ""
    if nominal and nominal.field_roles.modification_ordering:

        orderby_clause = "order by "
        if nominal.unique_on:
            orderby_clause += ", ".join(identifiers(nominal.unique_on)) + ", \n"
        orderby_clause += (
            " desc, ".join(identifiers(nominal.field_roles.modification_ordering))
            + " desc"
        )

    target_storage = ctx.execution_config.get_target_storage()
    sql = f"""
    select
        {distinct_clause}
        {cols}
    from {input.as_sql_from_stmt(target_storage)}
        {orderby_clause}
    """
    # TODO: clean up this hack
    sdf = SqlDataFunctionWrapper(sql)

    def noop(*args, **kwargs):
        return sql

    sdf.get_compiled_sql = noop
    return sdf(ctx, input=input)
