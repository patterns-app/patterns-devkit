from __future__ import annotations

import re
from dataclasses import dataclass
from re import Match
from typing import Any, Dict, List, Optional, Tuple

import sqlparse
from dags.core.data_block import DataBlock, DataBlockMetadata, StoredDataBlockMetadata
from dags.core.data_formats import DataFormat
from dags.core.pipe import DataInterfaceType, Pipe, PipeInterface, pipe_factory
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


@dataclass(frozen=True)
class TypedSqlStatement:
    cleaned_sql: str
    interface: PipeInterface


def annotation_from_comment_annotation(ann: str, **kwargs) -> PipeAnnotation:
    ann = ann.strip().strip("-# ")
    if not ann.startswith(":"):
        raise BadAnnotationException
    ann = ann.strip(": ")
    return PipeAnnotation.from_type_annotation(ann, **kwargs)


def extract_interface(
    sql: str,
    replace_with_names: Optional[Dict[str, str]] = None,
    ignore_jinja=True,
    comment_annotations=True,
) -> TypedSqlStatement:
    # debug = print
    # TODO: Bit of a nightmare. Need to extend a proper grammar/parser for this
    """
    Get all table names in a sql statement, optionally sub them with new names.
    Also extract comment-style ObjectType annotations if they exists.
    """

    @dataclass
    class TableParseState:
        prev_token: Optional[str] = None
        prev_token_was_select: bool = False
        table_identifier_stmt: bool = False
        table_identifier_required_next: bool = False
        previous_token_table_identifier: Optional[str] = None
        jinja_context_cnt: int = 0

    replace_with_names = replace_with_names or {}
    table_refs: Dict[str, Optional[str]] = {}
    output_annotation: Optional[str] = None
    state = TableParseState()
    new_sql: List[str] = []
    for stmt in sqlparse.parse(sql):
        for token in stmt.flatten():
            # print(token, table_stmt_required, table_stmt)
            if new_sql:
                state.prev_token = new_sql[-1]
            new_sql.append(str(token))
            # debug("\t\t", token, "\t\t", token.ttype, type(token))
            if token.ttype in tokens.Comment:
                # debug("comment", str(token), prev_select, prev_table_ref)
                if (
                    comment_annotations
                    and state.previous_token_table_identifier is not None
                ):
                    table_refs[state.previous_token_table_identifier] = str(token)
                    state.previous_token_table_identifier = None
                if comment_annotations and state.prev_token_was_select:
                    state.prev_token_was_select = False
                    output_annotation = str(token)
                continue
            if token.is_whitespace:
                continue
            t = str(token).lower()
            if t == ",":
                if state.table_identifier_stmt:
                    # debug("table comma")
                    state.table_identifier_required_next = True
                    continue
            state.previous_token_table_identifier = None
            state.prev_token_was_select = False
            # Skip jinja if present
            if t in ("%", "{", "%-") and state.prev_token == "{":
                state.jinja_context_cnt += 1
                continue
            if t == "}" and state.prev_token in ("}", "%", "-%"):
                state.jinja_context_cnt -= 1
                continue
            if state.jinja_context_cnt and ignore_jinja:
                # debug("\t", t, f"skip jinja {jinja}")
                continue
            if token.is_keyword:
                if "join" in t or "from" in t:
                    # debug("on", token)
                    state.table_identifier_stmt = True
                    state.table_identifier_required_next = True
                else:
                    if "select" == t:
                        state.prev_token_was_select = True
                    if not state.table_identifier_required_next:
                        # debug("off", token)
                        # Only turn off table_stmt if we don't require one
                        # Otherwise false positive here on table names that happen to be keywords
                        state.table_identifier_stmt = False
                    else:
                        # table name mistaken for keyword
                        token.ttype = tokens.Name
            if token.ttype in tokens.Name:
                if state.table_identifier_stmt:
                    # debug("found", token)
                    table_ref = str(token)
                    table_refs[table_ref] = None
                    state.previous_token_table_identifier = table_ref
                    state.table_identifier_required_next = False
                    if table_ref in replace_with_names:
                        new_sql.pop()
                        new_sql.append(
                            replace_with_names[table_ref] + f' as "{table_ref}"'
                        )
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
        if ann:
            try:
                # logger.debug(f"Found comment annotation in SQL: {ann}")
                inputs.append(annotation_from_comment_annotation(ann, name=name))
                continue
            except BadAnnotationException:
                pass
        inputs.append(PipeAnnotation.create(name=name, data_format_class="DataSet"))
    return TypedSqlStatement(
        cleaned_sql="".join(new_sql),
        interface=PipeInterface(inputs=inputs, output=output),
    )


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

        for i in inputs.values():
            if not isinstance(i, DataBlock):
                raise NotImplementedError(f"Unsupported input type {i}")
        sql = self.get_compiled_sql(ctx, inputs)
        # if ctx.resolved_output_otype is None:
        #     raise Exception("SQL pipe should always produce output!")

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
        return compile_jinja_sql(sql, sql_ctx)

    def get_typed_statement(
        self, inputs: Dict[str, DataBlock] = None,
    ) -> TypedSqlStatement:
        return extract_types(self.sql, self.get_input_table_names(inputs))

    def get_interface(self) -> PipeInterface:
        stmt = self.get_typed_statement()
        return stmt.interface


def sql_pipe_factory(
    key: str,
    sql: str = None,
    version: str = None,
    compatible_runtimes: str = None,  # TODO: engine support
    module_key: str = None,
    **kwargs,  # TODO: explicit options
) -> Pipe:
    if not sql:
        raise ValueError("Must give sql")
    return pipe_factory(
        SqlPipeWrapper(sql),
        key=key,
        compatible_runtimes=compatible_runtimes or "database",
        **kwargs,
    )


sql_pipe = sql_pipe_factory
