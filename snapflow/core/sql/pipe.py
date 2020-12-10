from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import sqlparse
from loguru import logger
from snapflow.core.data_block import (
    DataBlock,
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from snapflow.core.module import SnapflowModule
from snapflow.core.pipe import DataInterfaceType, Pipe, PipeInterface, pipe_factory
from snapflow.core.pipe_interface import (
    BadAnnotationException,
    PipeAnnotation,
    make_default_output_annotation,
)
from snapflow.core.runnable import PipeContext
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


@dataclass
class TableParseState:
    prev_token: Optional[str] = None
    prev_token_was_select: bool = False
    table_identifier_stmt: bool = False
    table_identifier_required_next: bool = False
    previous_token_table_identifier: Optional[str] = None
    jinja_context_cnt: int = 0


def make_typed_statement_from_parse(
    new_sql: List[str],
    table_identifier_annotations: Dict[str, Optional[str]],
    output_annotation: Optional[str],
):
    output = None
    if output_annotation:
        try:
            output = annotation_from_comment_annotation(output_annotation)
        except BadAnnotationException:
            pass
    if output is None:
        output = make_default_output_annotation()
    inputs = []
    for name, ann in table_identifier_annotations.items():
        if ann:
            try:
                # logger.debug(f"Found comment annotation in SQL: {ann}")
                inputs.append(annotation_from_comment_annotation(ann, name=name))
                continue
            except BadAnnotationException:
                pass
        inputs.append(PipeAnnotation.create(name=name, data_format_class="DataBlock"))
    return TypedSqlStatement(
        cleaned_sql="".join(new_sql),
        interface=PipeInterface(inputs=inputs, output=output),
    )


def skip_jinja(t: str, state: TableParseState, ignore_jinja: bool = True) -> bool:
    if t in ("%", "{", "%-") and state.prev_token == "{":
        state.jinja_context_cnt += 1
        return True
    if t == "}" and state.prev_token in ("}", "%", "-%"):
        state.jinja_context_cnt -= 1
        return True
    if state.jinja_context_cnt and ignore_jinja:
        # debug("\t", t, f"skip jinja {jinja}")
        return True
    return False


def extract_interface(
    sql: str,
    replace_with_names: Optional[Dict[str, str]] = None,
    ignore_jinja=True,
    comment_annotations=True,
) -> TypedSqlStatement:
    # TODO: Bit of a nightmare. Need to extend a proper grammar/parser for this
    """
    Get all table names in a sql statement, optionally sub them with new names.
    Also extract comment-style Schema annotations if they exists.
    """
    replace_with_names = replace_with_names or {}
    table_identifier_annotations: Dict[str, Optional[str]] = {}
    output_annotation: Optional[str] = None
    state = TableParseState()
    new_sql: List[str] = []
    for stmt in sqlparse.parse(sql):
        for token in stmt.flatten():
            if new_sql:
                state.prev_token = new_sql[-1]
            new_sql.append(str(token))
            if token.ttype in tokens.Comment:
                if (
                    comment_annotations
                    and state.previous_token_table_identifier is not None
                ):
                    table_identifier_annotations[
                        state.previous_token_table_identifier
                    ] = str(token)
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
                    state.table_identifier_required_next = True
                    continue
            state.previous_token_table_identifier = None
            state.prev_token_was_select = False
            # Skip jinja stmt if present
            if skip_jinja(t, state, ignore_jinja):
                continue
            if token.is_keyword:
                if "join" in t or "from" in t:
                    state.table_identifier_stmt = True
                    state.table_identifier_required_next = True
                else:
                    if "select" == t:
                        state.prev_token_was_select = True
                    if not state.table_identifier_required_next:
                        # Only turn off table_stmt if we don't require one
                        # Otherwise false positive here on table names that happen to be keywords
                        state.table_identifier_stmt = False
                    else:
                        # table name mistaken for keyword
                        token.ttype = tokens.Name
            if token.ttype in tokens.Name:
                if state.table_identifier_stmt:
                    table_ref = str(token)
                    table_identifier_annotations[table_ref] = None
                    state.previous_token_table_identifier = table_ref
                    state.table_identifier_required_next = False
                    if table_ref in replace_with_names:
                        new_sql.pop()
                        new_sql.append(
                            replace_with_names[table_ref] + f' as "{table_ref}"'
                        )
    return make_typed_statement_from_parse(
        new_sql, table_identifier_annotations, output_annotation
    )


def extract_types(
    sql: str, input_table_stmts: Dict[str, str] = None
) -> TypedSqlStatement:
    if input_table_stmts is None:
        input_table_stmts = {}
    return extract_interface(sql, input_table_stmts)


class SqlPipeWrapper:
    def __init__(self, sql: str):
        self.sql = sql

    def __call__(
        self, *args: PipeContext, **inputs: DataInterfaceType
    ) -> StoredDataBlockMetadata:
        ctx = args[0]
        if ctx.execution_context.current_runtime is None:
            raise Exception("Current runtime not set")

        sql = self.get_compiled_sql(ctx, inputs)
        # if ctx.resolved_output_schema is None:
        #     raise Exception("SQL pipe should always produce output!")

        db_api = ctx.execution_context.current_runtime.get_database_api(
            ctx.execution_context.env
        )
        logger.debug(
            f"Resolved in sql pipe {ctx.runnable.bound_interface.resolve_nominal_output_schema( ctx.worker.env )}"
        )
        block, sdb = db_api.create_data_block_from_sql(
            sql,
            nominal_schema=ctx.runnable.bound_interface.resolve_nominal_output_schema(
                ctx.worker.env
            ),
            created_by_node_key=ctx.runnable.node_key,
        )

        return sdb

    def get_input_table_stmts(
        self,
        inputs: Dict[str, DataBlock] = None,
    ) -> Dict[str, str]:
        if inputs is None:
            return {}
        table_stmts = {}
        for input_name, block in inputs.items():
            if isinstance(block, DataBlock):
                schema = block.as_table()
                table_stmts[input_name] = schema.get_table_stmt()
        return table_stmts

    def get_compiled_sql(
        self,
        ctx: PipeContext,
        inputs: Dict[str, DataBlock] = None,
    ):
        from snapflow.core.sql.utils import compile_jinja_sql

        sql = self.get_typed_statement(inputs).cleaned_sql
        sql_ctx = dict(
            execution_context=ctx.execution_context,
            worker=ctx.worker,
            runnable=ctx.runnable,
            inputs={i.name: i for i in ctx.inputs},
            # TODO: we haven't logged the input blocks yet (in the case of a stream) so we can't
            #    resolve the nominal output schema at this point. But it is _possible_ if necessary -- is it?
            # output_schema=ctx.runnable.bound_interface.resolve_nominal_output_schema(
            #     ctx.worker.env
            # ),
        )
        return compile_jinja_sql(sql, sql_ctx)

    def get_typed_statement(
        self,
        inputs: Dict[str, DataBlock] = None,
    ) -> TypedSqlStatement:
        return extract_types(self.sql, self.get_input_table_stmts(inputs))

    def get_interface(self) -> PipeInterface:
        stmt = self.get_typed_statement()
        return stmt.interface


def sql_pipe_factory(
    name: str,
    sql: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    version: str = None,
    compatible_runtimes: str = None,  # TODO: engine support
    **kwargs,  # TODO: explicit options
) -> Pipe:
    if not sql:
        raise ValueError("Must provide sql")
    return pipe_factory(
        SqlPipeWrapper(sql),
        name=name,
        module=module,
        compatible_runtimes=compatible_runtimes or "database",
        **kwargs,
    )


sql_pipe = sql_pipe_factory
