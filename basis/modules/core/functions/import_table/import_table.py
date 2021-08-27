from __future__ import annotations

import json
from typing import Optional

from basis import Context, basis
from basis.core.sql.sql_function import SqlFunctionWrapper
from dcp.utils.common import ensure_bool


@function(
    namespace="core",
    display_name="Import non-basis database table",
    required_storage_classes=["database"],
)
def import_table(ctx: Context, table_name: str, copy: bool = True):
    """
    Imports a non-basis table from an existing storage, optionally making a copy
    or importing as-is. Warning: If you do not make a copy, potentially breaking
    basis contract of immutable data.

    Params:
        table_name: Name of the database table (on existing database)
        copy: If true, make a copy snapshot of the table, otherwise import as an alias
    """
    target_storage = ctx.execution_cfg.get_target_storage()
    if ensure_bool(copy):
        as_identifier = target_storage.get_api().get_quoted_identifier
        sql = f"select * from {as_identifier(table_name)}"
        # TODO: DRY this pattern
        sdf = SqlFunctionWrapper(sql)

        def get_sql(*args, **kwargs):
            return sql

        sdf.get_compiled_sql = get_sql
        return sdf(ctx)
    else:
        ctx.emit(
            name=table_name,
            storage=target_storage,
            data_format="table",
            create_alias_only=True,
        )


@basis(
    namespace="core",
    display_name="Import non-basis database table",
    required_storage_classes=["database"],
)
def import_table(ctx: Context, table_name: str, copy: bool = True):
    pass
