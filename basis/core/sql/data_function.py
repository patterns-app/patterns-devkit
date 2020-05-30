from __future__ import annotations

import re
from dataclasses import dataclass
from re import Match
from typing import Any, Dict, Tuple

from basis.core.data_block import DataBlock, DataBlockMetadata, StoredDataBlockMetadata
from basis.core.data_format import DataFormat
from basis.core.data_function import (
    DataFunction,
    DataFunctionInterface,
    TypedDataAnnotation,
    re_type_hint,
)
from basis.core.runnable import DataFunctionContext

# NB: It's important that these regexes can't combinatorially explode (they will be parsing user input)
from basis.utils.common import md5_hash

word_start = r"(?:(?<=\s)|(?<=^))"
word_end = r"(?=\s|$|,)"
table_stmt_start = r"(from|join|,)"
input_type_stmt = re_type_hint.pattern
optional_type_stmt = r"(?P<optional>(Optional)\[)?(?P<type>(\w+\.)?\w+)\]?"
simple_type_stmt = r"(?P<type>(\w+\.)?\w+)"
table_alias = r"\s+as\s+(?P<alias>\w+)"  # TODO: ONLY supports explicit `as` aliases atm
select_type_stmt = re.compile(
    rf"{word_start}(?P<select>select)\:{simple_type_stmt}{word_end}", re.I | re.M
)
table_type_stmt = re.compile(
    rf"""
        (?P<table_statement>
            (?P<table_preamble>
                {table_stmt_start}\s+
            )
            {word_start}
            (?P<table_name>\w+)
        )
        \:{optional_type_stmt}
        ({table_alias})?
        {word_end}
    """,
    re.I | re.M | re.X,
)


@dataclass(frozen=True)
class TypedSqlStatement:
    cleaned_sql: str
    interface: DataFunctionInterface


def extract_and_replace_sql_input(
    sql: str, m: Match, input_table_names: Dict[str, str]
) -> Tuple[str, TypedDataAnnotation]:
    # TODO: strip sql comments
    groups: Dict[str, str] = m.groupdict()
    name = groups["table_name"]
    is_optional = groups.get("optional")
    otype = groups["type"]
    annotation = f"DataBlock[{otype}]"
    if is_optional:
        annotation = f"Optional[{annotation}]"
    tda = TypedDataAnnotation.from_type_annotation(
        annotation, name=name  # TODO: DataSet
    )
    # By default, just replace with existing table statement and alias
    table_stmt = r"\g<table_statement> \g<alias>"
    if name in input_table_names:
        alias = groups.get("alias")
        table_alias = alias or name
        table_stmt = rf"\g<table_preamble> {input_table_names[name]} as {table_alias}"
    sql = table_type_stmt.sub(table_stmt, sql, count=1)
    return sql, tda


def extract_types(
    sql: str, input_table_names: Dict[str, str] = None
) -> TypedSqlStatement:
    if input_table_names is None:
        input_table_names = {}
    m = select_type_stmt.search(sql)
    output = None
    if m is not None:
        output_type = m.groupdict()["type"]
        sql = select_type_stmt.sub(r"\g<select>", sql, 1)
        # output = TypedDataAnnotation(
        #     data_block_class="DataSet", otype_like=output_type
        # )
        output = TypedDataAnnotation.from_type_annotation(
            f"DataBlock[{output_type}]"
        )  # TODO: DataSet
    input_types = []
    for _ in range(
        1000
    ):  # If you have more than 1000 table references something is probably wrong
        m = table_type_stmt.search(sql)
        if m is None:
            break
        sql, tda = extract_and_replace_sql_input(sql, m, input_table_names)
        input_types.append(tda)
    return TypedSqlStatement(
        cleaned_sql=sql,
        interface=DataFunctionInterface(inputs=input_types, output=output),
    )


class SqlDataFunction(DataFunction):
    def __init__(self, sql: str, key: str = None, **kwargs: Any):
        super().__init__(
            key or md5_hash(sql)
        )  # TODO: is hashing the right thing to do?
        self.sql = sql
        self._kwargs = kwargs

    def __call__(
        self, ctx: DataFunctionContext, **inputs: DataBlock
    ) -> StoredDataBlockMetadata:

        if ctx.execution_context.current_runtime is None:
            raise Exception("Current runtime not set")

        # if ctx.runtime.runtime_class != RuntimeClass.DATABASE:
        #     raise Exception(
        #         "Incompatible Runtime"
        #     )  # TODO: Everyone SQL and You Can Too!
        sql = self.get_compiled_sql(ctx, inputs)
        output = ctx.runnable.datafunction_interface.output
        if output is None:
            raise Exception("SQL function should always produce output!")
        block = DataBlockMetadata(otype_uri=output.otype_uri)
        storage_url = ctx.execution_context.current_runtime.as_storage().url
        sdb = StoredDataBlockMetadata(
            data_block=block,
            storage_url=storage_url,
            data_format=DataFormat.DATABASE_TABLE,
        )
        ctx.execution_context.add(sdb)

        # TODO: oof this is doozy, will get fixed as part of runtime re-think
        db_api = ctx.execution_context.current_runtime.get_database_api(
            ctx.execution_context.env
        )
        db_api.insert_sql(sdb, sql)
        # TODO: handle anonymous ObjectTypes in SQL
        return sdb

    def get_input_table_names(
        self, inputs: Dict[str, DataBlock] = None,
    ) -> Dict[str, str]:
        if inputs is None:
            return {}
        table_names = {}
        for input_name, block in inputs.items():
            otype = block.as_table()
            table_names[input_name] = otype.table_name
        return table_names

    def get_compiled_sql(
        self, ctx: DataFunctionContext, inputs: Dict[str, DataBlock] = None,
    ):
        from basis.core.sql.utils import compile_jinja_sql

        sql = self.get_typed_statement(inputs).cleaned_sql
        sql_ctx = dict(
            execution_context=ctx.execution_context,
            worker=ctx.worker,
            runnable=ctx.runnable,
            inputs={i.name: i for i in ctx.inputs},
            output=ctx.output,
        )
        # sql_ctx.update(inputs) # TODO: decide what is in the sql jinja ctx. usability is key
        return compile_jinja_sql(sql, sql_ctx)

    def get_typed_statement(
        self, inputs: Dict[str, DataBlock] = None,
    ) -> TypedSqlStatement:
        return extract_types(self.sql, self.get_input_table_names(inputs))

    def get_interface(self) -> DataFunctionInterface:
        stmt = self.get_typed_statement()
        return stmt.interface


sql_datafunction = SqlDataFunction
