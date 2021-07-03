from __future__ import annotations

import json
from typing import Optional

from basis import DataFunctionContext
from basis.core.function import datafunction
from basis.core.sql.sql_function import SqlDataFunctionWrapper


@datafunction(
    namespace="core",
    display_name="Import external database table (one not produced by basis)",
    required_storage_classes=["database"],
)
def import_table(ctx: DataFunctionContext, table_name: str):
    target_storage = ctx.execution_config.get_target_storage()
    as_identifier = target_storage.get_api().get_quoted_identifier
    sql = f"select * from {as_identifier(table_name)}"
    # TODO: DRY this pattern
    sdf = SqlDataFunctionWrapper(sql)

    def get_sql(*args, **kwargs):
        return sql

    sdf.get_compiled_sql = get_sql
    return sdf(ctx)
