from __future__ import annotations
from snapflow.core.sql.sql_function import SqlDataFunctionWrapper
from typing import Optional

from commonmodel.base import Schema

from snapflow import datafunction, DataFunctionContext, DataBlock


@datafunction(
    namespace="core",
    display_name="Dedupe Table (keep latest)",
    required_storage_engines=["postgres"],
    # TODO: requires postgres StorageEngine
)
def dedupe_keep_latest_sql(
    ctx: DataFunctionContext, input: DataBlock[T]
) -> DataBlock[T]:
    # return "dedupe_keep_latest_sql.sql"
    nominal: Optional[Schema] = None
    if input.nominal_schema_key:
        nominal = input.nominal_schema
    distinct_clause = ""
    if nominal and nominal.unique_on:
        distinct_clause = f" distinct on ({', '.join(nominal.unique_on)})"
    realized: Schema = input.realized_schema
    cols = ", ".join(realized.field_names())

    orderby_clause = ""
    if nominal and nominal.field_roles.modification_ordering:

        orderby_clause = "order by "
        if nominal.unique_on:
            orderby_clause += ", ".join(nominal.unique_on) + ", \n"
        orderby_clause += (
            " desc, ".join(nominal.field_roles.modification_ordering) + " desc"
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
