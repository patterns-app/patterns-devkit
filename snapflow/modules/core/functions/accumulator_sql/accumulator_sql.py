from __future__ import annotations

from typing import Dict, List

from commonmodel import DEFAULT_FIELD_TYPE, FieldType
from dcp.data_format.formats import DatabaseTable
from dcp.data_format.formats.database.base import field_type_to_sqlalchemy_type
from dcp.data_format.handler import get_handler_for_name
from snapflow import DataFunctionContext
from snapflow.core.data_block import SelfReference, Stream
from snapflow.core.function import datafunction
from snapflow.core.sql.sql_function import SqlDataFunctionWrapper, sql_datafunction
from snapflow.utils.typing import T
from sqlalchemy.sql import cast, select


def merge_field_types(ft_base: FieldType, ft_new: FieldType) -> FieldType:
    if ft_base.name in ft_new.castable_to_types:
        return ft_base
    if ft_new.name in ft_base.castable_to_types:
        return ft_new
    return DEFAULT_FIELD_TYPE


def field_sql_with_cast(name: str, ftype: FieldType, dialect=None) -> str:
    sa_type = field_type_to_sqlalchemy_type(ftype)
    sa_dialect_type = sa_type.compile(dialect=dialect)
    return f"cast({name} as {sa_dialect_type})"
    # s = str(cast("placeholder", sa_type).compile(dialect=dialect))
    # if "% " in s:
    #     s = s.replace("%", name)
    # elif "? " in s:
    #     s = s.replace("?", name)
    # return s


@datafunction(namespace="core", display_name="Accumulate sql tables")
def accumulator_sql(
    ctx: DataFunctionContext,
    input: Stream[T],
    previous: SelfReference[T] = None,
) -> DatabaseTable[T]:
    """
    Critical core data function. Handles a scary operation: merging a stream of data blocks
    into one. These data blocks can have arbitrary realized schemas, so
    we have to handle field name and data type differences gracefully.
    """
    cols: List[str] = []
    col_types: Dict[str, FieldType] = {}
    blocks = []
    if previous:
        blocks.append(previous)
    blocks.extend(input)
    target_storage = ctx.execution_config.get_target_storage()
    as_identifier = target_storage.get_api().get_quoted_identifier
    select_stmts = []
    for block in blocks:
        for f in block.realized_schema.fields:
            if f.name not in cols:
                cols.append(f.name)
            if f.name in col_types:
                col_types[f.name] = merge_field_types(col_types[f.name], f.field_type)
            else:
                col_types[f.name] = f.field_type
    for block in blocks:
        col_sql = []
        for col in cols:
            if col in block.realized_schema.field_names():
                f = block.realized_schema.get_field(col)
                # if f.field_type == col_types[f.name]:
                #     cast_sql = col
                # else:
                # TODO: Always cast?
                dialect = target_storage.get_api().get_engine().dialect
                cast_sql = field_sql_with_cast(
                    as_identifier(f.name), col_types[f.name], dialect
                )
                col_sql.append(f"{cast_sql} as {as_identifier(col)}")
            else:
                col_sql.append("null as " + as_identifier(col))
        select_stmts.append(
            "select "
            + ",\n".join(col_sql)
            + " from "
            + block.as_sql_from_stmt(target_storage)
        )
    sql = " union all ".join(select_stmts)
    sdf = SqlDataFunctionWrapper(sql)

    def noop(*args, **kwargs):
        return sql

    sdf.get_compiled_sql = noop
    return sdf(ctx, input=input, previous=previous)
