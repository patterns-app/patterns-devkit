from __future__ import annotations

import re
from dataclasses import dataclass
from re import Match
from typing import Any, Dict, List, Optional, Tuple

import sqlparse
from dags.core.data_block import DataBlock, DataBlockMetadata, StoredDataBlockMetadata
from dags.core.data_formats import DataFormat
from dags.core.pipe import (
    DataInterfaceType,
    PipeDefinition,
    PipeInterface,
    pipe_definition_factory,
)
from dags.core.pipe_interface import (
    BadAnnotationException,
    PipeAnnotation,
    re_type_hint,
)
from dags.core.runnable import PipeContext

# NB: It's important that these regexes can't combinatorially explode (they will be parsing user input)
from dags.core.runtime import RuntimeClass
from dags.utils.common import md5_hash
from sqlparse import tokens

# word_start = r"(?:(?<=\s)|(?<=^))"
# word_end = r"(?=\s|$|,)"
# table_stmt_start = r"(from|join|,)"
# input_type_stmt = re_type_hint.pattern
# optional_type_stmt = r"(?P<optional>(Optional)\[)?(?P<type>(\w+\.)?\w+)\]?"
# simple_type_stmt = r"(?P<type>(\w+\.)?\w+)"
# table_alias = r"\s+as\s+(?P<alias>\w+)"  # TODO: ONLY supports explicit `as` aliases atm
# select_type_stmt = re.compile(
#     rf"{word_start}(?P<select>select)\:{simple_type_stmt}{word_end}", re.I | re.M
# )
# table_type_stmt = re.compile(
#     rf"""
#         (?P<table_statement>
#             (?P<table_preamble>
#                 {table_stmt_start}\s+
#             )
#             {word_start}
#             (?P<table_name>\w+)
#         )
#         \:{optional_type_stmt}
#         ({table_alias})?
#         {word_end}
#     """,
#     re.I | re.M | re.X,
# )


@dataclass(frozen=True)
class TypedSqlStatement:
    cleaned_sql: str
    interface: PipeInterface


def annotation_from_comment_annotation(ann: str, **kwargs) -> PipeAnnotation:
    ann = ann.strip().strip("-# ")
    # TODO: make this more explicit? So we don't have accidental annotation (any one word comment would pass...)
    # if not ann.startswith(":"):
    #     raise BadAnnotationException
    # ann = ann.strip(": ")
    return PipeAnnotation.from_type_annotation(ann, **kwargs)


def extract_interface(
    sql: str,
    replace_with_names: Optional[Dict[str, str]] = None,
    ignore_jinja=True,
    comment_annotations=True,
) -> TypedSqlStatement:
    # debug = print
    # TODO: Say it with me: un - main - tain - able!
    replace_with_names = replace_with_names or {}
    table_refs: Dict[str, Optional[str]] = {}
    output_annotation: Optional[str] = None
    table_stmt = False
    table_stmt_required = False
    jinja = 0
    new_sql = []
    prev = None
    prev_table_ref = None
    prev_select = False
    for stmt in sqlparse.parse(sql):
        for token in stmt.flatten():
            # print(token, table_stmt_required, table_stmt)
            if new_sql:
                prev = new_sql[-1]
            new_sql.append(str(token))
            # debug("\t\t", token, "\t\t", token.ttype, type(token))
            if token.ttype in tokens.Comment:
                # debug("comment", str(token), prev_select, prev_table_ref)
                if comment_annotations and prev_table_ref:
                    table_refs[prev_table_ref] = str(token)
                    prev_table_ref = None
                if comment_annotations and prev_select:
                    prev_select = False
                    output_annotation = str(token)
                continue
            if token.is_whitespace:
                continue
            t = str(token).lower()
            if t == ",":
                if table_stmt:
                    # debug("table comma")
                    table_stmt_required = True
                    continue
            prev_table_ref = None
            prev_select = False
            # Skip jinja if present
            if t in ("%", "{", "%-") and prev == "{":
                jinja += 1
                continue
            if t == "}" and prev in ("}", "%", "-%"):
                jinja -= 1
                continue
            if jinja and ignore_jinja:
                # debug("\t", t, f"skip jinja {jinja}")
                continue
            if token.is_keyword:
                if "join" in t or "from" in t:
                    # debug("on", token)
                    table_stmt = True
                    table_stmt_required = True
                else:
                    if "select" == t:
                        prev_select = True
                    if not table_stmt_required:
                        # debug("off", token)
                        # Only turn off table_stmt if we don't require one
                        # Otherwise false positive here on table names that happen to be keywords
                        table_stmt = False
                    else:
                        # table name mistaken for keyword
                        token.ttype = tokens.Name
            if token.ttype in tokens.Name:
                if table_stmt:
                    # debug("found", token)
                    table_ref = str(token)
                    table_refs[table_ref] = None
                    prev_table_ref = table_ref
                    table_stmt_required = False
                    if table_ref in replace_with_names:
                        new_sql.pop()
                        new_sql.append(replace_with_names[table_ref])
    output = None
    if output_annotation:
        try:
            output = annotation_from_comment_annotation(output_annotation)
        except BadAnnotationException:
            pass
    if output is None:
        output = PipeAnnotation.create(data_format_class="DataSet")
    inputs = []
    for name, ann in table_refs.items():
        # print("input:", name, ann)
        if ann:
            try:
                inputs.append(annotation_from_comment_annotation(ann, name=name))
                continue
            except BadAnnotationException:
                pass
        inputs.append(PipeAnnotation.create(name=name, data_format_class="DataSet"))
    return TypedSqlStatement(
        cleaned_sql="".join(new_sql),
        interface=PipeInterface(inputs=inputs, output=output),
    )


# def extract_and_replace_sql_input(
#     sql: str, m: Match, input_table_names: Dict[str, str]
# ) -> Tuple[str, PipeAnnotation]:
#     # TODO: strip sql comments
#     groups: Dict[str, str] = m.groupdict()
#     name = groups["table_name"]
#     is_optional = groups.get("optional")
#     otype = groups["type"]
#     annotation = f"DataBlock[{otype}]"
#     if is_optional:
#         annotation = f"Optional[{annotation}]"
#     tda = PipeAnnotation.from_type_annotation(
#         annotation, name=name  # TODO: DataSet
#     )
#     # By default, just replace with existing table statement and alias
#     table_stmt = r"\g<table_statement> \g<alias>"
#     if name in input_table_names:
#         alias = groups.get("alias")
#         table_alias = alias or name
#         table_stmt = rf"\g<table_preamble> {input_table_names[name]} as {table_alias}"
#     sql = table_type_stmt.sub(table_stmt, sql, count=1)
#     return sql, tda
#
#
# def extract_types(
#     sql: str, input_table_names: Dict[str, str] = None
# ) -> TypedSqlStatement:
#     if input_table_names is None:
#         input_table_names = {}
#     m = select_type_stmt.search(sql)
#     output = None
#     if m is not None:
#         output_type = m.groupdict()["type"]
#         sql = select_type_stmt.sub(r"\g<select>", sql, 1)
#         # output = PipeAnnotation(
#         #     data_format_class="DataSet", otype_like=output_type
#         # )
#         output = PipeAnnotation.from_type_annotation(
#             f"DataBlock[{output_type}]"
#         )  # TODO: DataSet
#     input_types = []
#     for _ in range(
#         1000
#     ):  # If you have more than 1000 table references something is probably wrong
#         m = table_type_stmt.search(sql)
#         if m is None:
#             break
#         sql, tda = extract_and_replace_sql_input(sql, m, input_table_names)
#         input_types.append(tda)
#     return TypedSqlStatement(
#         cleaned_sql=sql,
#         interface=PipeInterface(inputs=input_types, output=output),
#     )


def extract_types(
    sql: str, input_table_names: Dict[str, str] = None
) -> TypedSqlStatement:
    if input_table_names is None:
        input_table_names = {}
    return extract_interface(sql, input_table_names)


class SqlPipeWrapper:
    def __init__(self, sql: str):
        self.sql = sql

    def __call__(
        self, *args: PipeContext, **inputs: DataInterfaceType
    ) -> StoredDataBlockMetadata:
        ctx = args[0]
        if ctx.execution_context.current_runtime is None:
            raise Exception("Current runtime not set")

        # if ctx.runtime.runtime_class != RuntimeClass.DATABASE:
        #     raise Exception(
        #         "Incompatible Runtime"
        #     )  # TODO: Everyone SQL and You Can Too!
        for i in inputs.values():
            if not isinstance(i, DataBlock):
                raise NotImplementedError(f"Unsupported input type {i}")
        sql = self.get_compiled_sql(ctx, inputs)
        # if ctx.resolved_output_otype is None:
        #     raise Exception("SQL pipe should always produce output!")

        # TODO: oof this is doozy, will get fixed as part of runtime re-think
        db_api = ctx.execution_context.current_runtime.get_database_api(
            ctx.execution_context.env
        )
        block, sdb = db_api.create_data_block_from_sql(
            ctx.execution_context.metadata_session,
            sql,
            # expected_otype=ctx.resolved_output_otype,
        )

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
        self, ctx: PipeContext, inputs: Dict[str, DataBlock] = None,
    ):
        from dags.core.sql.utils import compile_jinja_sql

        sql = self.get_typed_statement(inputs).cleaned_sql
        sql_ctx = dict(
            execution_context=ctx.execution_context,
            worker=ctx.worker,
            runnable=ctx.runnable,
            inputs={i.name: i for i in ctx.inputs},
            # output_otype=ctx.resolved_output_otype,
            # output_otype=ctx.realized_output_otype,
        )
        # sql_ctx.update(inputs) # TODO: decide what is in the sql jinja ctx. usability is key
        return compile_jinja_sql(sql, sql_ctx)

    def get_typed_statement(
        self, inputs: Dict[str, DataBlock] = None,
    ) -> TypedSqlStatement:
        return extract_types(self.sql, self.get_input_table_names(inputs))

    def get_interface(self) -> PipeInterface:
        stmt = self.get_typed_statement()
        return stmt.interface


def sql_pipe_factory(
    name: str,
    sql: str = None,
    version: str = None,
    compatible_runtimes: str = None,  # TODO: engine support
    module_name: str = None,
    **kwargs,  # TODO: explicit options
) -> PipeDefinition:
    if not sql:
        raise ValueError("Must give sql")
    return pipe_definition_factory(
        SqlPipeWrapper(sql),
        name=name,
        module_name=module_name,
        version=version,
        compatible_runtimes=compatible_runtimes or "database",
        **kwargs,
    )


sql_pipe = sql_pipe_factory
