from __future__ import annotations
from pathlib import Path

import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from functools import partial
from snapflow.core.snap_package import load_file
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import sqlparse
from commonmodel.base import SchemaTranslation
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass, Storage
from dcp.storage.database.utils import column_map, compile_jinja_sql
from dcp.utils.common import rand_str
from loguru import logger
from snapflow.core.data_block import (
    DataBlock,
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from snapflow.core.environment import Environment
from snapflow.core.execution.execution import SnapContext
from snapflow.core.module import SnapflowModule
from snapflow.core.node import DataBlockLog
from snapflow.core.runtime import DatabaseRuntimeClass, RuntimeClass
from snapflow.core.snap import DataInterfaceType, Parameter, _Snap, snap_factory
from snapflow.core.snap_interface import (
    DEFAULT_CONTEXT,
    DEFAULT_INPUT_ANNOTATION,
    BadAnnotationException,
    DeclaredInput,
    DeclaredSnapInterface,
    ParsedAnnotation,
    make_default_output,
    parse_annotation,
    snap_input_from_annotation,
    snap_output_from_annotation,
)
from snapflow.core.streams import DataBlockStream, ManagedDataBlockStream
from sqlparse import tokens


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


def parse_sql_annotation(ann: str) -> ParsedAnnotation:
    if "[" in ann:
        # If type annotation is complex, parse it
        parsed = parse_annotation(ann)
    else:
        # If it's just a simple word, then assume it is a Schema name
        parsed = ParsedAnnotation(schema_like=ann, data_format_class="DataBlock")
    return parsed


@dataclass(frozen=True)
class AnnotatedSqlTable:
    name: str
    annotation: Optional[str] = None


@dataclass(frozen=True)
class AnnotatedParam:
    name: str
    annotation: Optional[str] = None


@dataclass(frozen=True)
class ParsedSqlStatement:
    original_sql: str
    sql_with_jinja_vars: str
    found_tables: Optional[Dict[str, AnnotatedSqlTable]] = None
    found_params: Optional[Dict[str, AnnotatedParam]] = None
    output_annotation: Optional[str] = None

    def as_interface(self) -> DeclaredSnapInterface:
        inputs = []
        for name, table in self.found_tables.items():
            if table.annotation:
                ann = parse_sql_annotation(table.annotation)
            else:
                ann = parse_annotation(DEFAULT_INPUT_ANNOTATION)
            inpt = snap_input_from_annotation(ann, name=name)
            inputs.append(inpt)
        if self.output_annotation:
            output = snap_output_from_annotation(
                parse_sql_annotation(self.output_annotation)
            )
        else:
            output = make_default_output()
        return DeclaredSnapInterface(
            inputs=inputs, output=output, context=DEFAULT_CONTEXT,
        )


def regex_repalce_match(s, m, r) -> str:
    return s[: m.start()] + r + s[m.end() :]


def extract_param_annotations(sql: str) -> ParsedSqlStatement:
    """
    Extract snapflow-specific tokens:
        - parameters, indicated by an opening colon, `:param1` and optional type `:param1:datetime`
        - inputs, automatically inferred `input1` but can have optional schema `input1:Transaction`
        - output is annoatated on the select keyword: `select:Transaction`
    """
    param_re = re.compile(r"(?P<whitespace>^|\s):(?P<name>\w+)(:(?P<datatype>\w+))?")
    params = {}
    sql_with_jinja_vars = sql
    while True:
        m = param_re.search(sql_with_jinja_vars)
        if m is None:
            break
        d = m.groupdict()
        params[d["name"]] = AnnotatedParam(name=d["name"], annotation=d.get("datatype"))
        jinja = " {{ params['%s'] }}" % d["name"]
        sql_with_jinja_vars = regex_repalce_match(sql_with_jinja_vars, m, jinja)
    return ParsedSqlStatement(
        original_sql=sql, sql_with_jinja_vars=sql_with_jinja_vars, found_params=params,
    )


def extract_table_annotations(sql: str) -> ParsedSqlStatement:
    table_re = re.compile(r"(^|\s)(?P<name>[A-z0-9_.]+):(?P<schema>[A-z0-9_\]\].]+)")
    tables = {}
    output_annotation = None
    sql_with_jinja_vars = sql
    while True:
        m = table_re.search(sql_with_jinja_vars)
        if m is None:
            break
        d = m.groupdict()
        if d["name"].lower() == "select":
            output_annotation = d["schema"]
            jinja = d["name"]
        else:
            tables[d["name"]] = AnnotatedSqlTable(
                name=d["name"], annotation=d["schema"]
            )
            jinja = " {{ inputs['%s'] }} as %s" % (d["name"], d["name"])
        sql_with_jinja_vars = regex_repalce_match(sql_with_jinja_vars, m, jinja)
    return ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=sql_with_jinja_vars,
        found_tables=tables,
        output_annotation=output_annotation,
    )


@dataclass
class TableParseState:
    prev_token: Optional[str] = None
    table_identifier_stmt: bool = False
    table_identifier_required_next: bool = False
    jinja_context_cnt: int = 0


def extract_tables(
    sql: str, replace_with_inputs_jinja: bool = True
) -> ParsedSqlStatement:
    """
    Extract tables that have no annotation.
    Table aliases are NOT supported (the name is already an alias!).
    """
    found_tables = {}
    state = TableParseState()
    new_sql: List[str] = []
    for stmt in sqlparse.parse(sql):
        for token in stmt.flatten():
            if new_sql:
                # Set previous token
                state.prev_token = new_sql[-1]
            # Add token to new_sql
            new_sql.append(str(token))
            if token.is_whitespace:
                continue
            t = str(token).lower()
            if t == ",":
                if state.table_identifier_stmt:
                    state.table_identifier_required_next = True
                    continue
            # Skip jinja stmt if present
            if skip_jinja(t, state):
                continue
            if token.is_keyword:
                if "join" in t or "from" in t:
                    state.table_identifier_stmt = True
                    state.table_identifier_required_next = True
                else:
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
                    found_tables[table_ref] = AnnotatedSqlTable(name=table_ref)
                    state.table_identifier_required_next = False
                    if replace_with_inputs_jinja:
                        new_sql.pop()
                        new_sql.append(
                            "{{ inputs['%s'] }} as %s" % (table_ref, table_ref)
                        )
    return ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars="".join(new_sql),
        found_tables=found_tables,
    )


def parse_sql_statement(sql: str, autodetect_tables: bool = True) -> ParsedSqlStatement:
    param_parse = extract_param_annotations(sql)
    table_parse = extract_table_annotations(param_parse.sql_with_jinja_vars)
    output = table_parse.output_annotation
    if not table_parse.found_tables and autodetect_tables:
        table_parse = extract_tables(table_parse.sql_with_jinja_vars)
    return ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=table_parse.sql_with_jinja_vars,
        found_params=param_parse.found_params,
        found_tables=table_parse.found_tables,
        output_annotation=output,
    )


def params_as_sql(ctx: SnapContext) -> Dict[str, Any]:
    param_values = ctx.get_params()
    sql_params = {}
    for k, v in param_values.items():
        p = ctx.snap.get_param(k)
        if p.datatype == "raw":
            sql_params[k] = v
            continue
        # First convert things to strings where makes sense
        if isinstance(v, datetime):
            v = v.isoformat()
        if isinstance(v, date):
            v = v.strftime("%Y-%m-%d")
        # Then quote string
        if isinstance(v, str):
            v = "'" + v + "'"
        sql_params[k] = v
    return sql_params


def apply_schema_translation_as_sql(
    env: Environment, name: str, translation: SchemaTranslation
) -> str:
    if not translation.from_schema_key:
        raise NotImplementedError(
            f"Schema translation must provide `from_schema` when translating a database table {translation}"
        )
    sql = column_map(
        name,
        env.get_schema(translation.from_schema_key).field_names(),
        translation.as_dict(),
    )
    table_stmt = f"""
        (
            {sql}
        ) as __translated
        """
    return table_stmt


class SqlSnapWrapper:
    def __init__(self, sql: str, autodetect_inputs: bool = True):
        self.sql = sql
        self.autodetect_inputs = autodetect_inputs

    def __call__(self, *args: SnapContext, **inputs: DataInterfaceType):
        ctx: SnapContext = args[0]
        # if ctx.run_context.current_runtime is None:
        #     raise Exception("Current runtime not set")

        # for input in inputs.values():
        #     if isinstance(input, ManagedDataBlockStream):
        #         dbs = input
        #     elif isinstance(input, DataBlock):
        #         dbs = [input]
        #     else:
        #         raise
        #     for db in dbs:
        #         assert db.has_format(DatabaseTableFormat)

        # TODO: way to specify more granular storage requirements (engine, engine version, etc)
        storage = ctx.execution_context.target_storage
        for storage in [
            ctx.execution_context.target_storage
        ] + ctx.execution_context.storages:
            if storage.storage_engine.storage_class == DatabaseStorageClass:
                break
        else:
            raise Exception("No database storage found, cannot exeucte snap sql")

        sql = self.get_compiled_sql(ctx, storage, inputs)

        db_api = storage.get_api()
        logger.debug(
            f"Resolved in sql snap {ctx.bound_interface.resolve_nominal_output_schema( ctx.env)}"
        )
        tmp_name = f"_tmp_{rand_str(10)}".lower()
        sql = db_api.clean_sub_sql(sql)
        create_sql = f"""
        create table {tmp_name} as
        select
        *
        from (
        {sql}
        ) as __sub
        """
        db_api.execute_sql(create_sql)
        ctx.emit(name=tmp_name, storage=storage, data_format=DatabaseTableFormat)
        # block, sdb = create_data_block_from_sql(
        #     ctx.env,
        #     sql,
        #     db_api=db_api,
        #     nominal_schema=ctx.bound_interface.resolve_nominal_output_schema(ctx.env),
        #     created_by_node_key=ctx.node.key,
        # )

    def get_input_table_stmts(
        self, ctx: SnapContext, storage: Storage, inputs: Dict[str, DataBlock] = None,
    ) -> Dict[str, str]:
        if inputs is None:
            return {}
        table_stmts = {}
        for input_name, block in inputs.items():
            if isinstance(block, DataBlock):
                table_stmts[input_name] = block.as_sql_from_stmt(storage)
        return table_stmts

    def get_compiled_sql(
        self, ctx: SnapContext, storage: Storage, inputs: Dict[str, DataBlock] = None,
    ):

        parsed = self.get_parsed_statement()
        input_sql = self.get_input_table_stmts(ctx, storage, inputs)
        sql_ctx = dict(
            ctx=ctx,
            inputs=input_sql,
            input_objects={i.name: i for i in ctx.inputs},
            params=params_as_sql(ctx),
            storage=storage,
            # TODO: we haven't logged the input blocks yet (in the case of a stream) so we can't
            #    resolve the nominal output schema at this point. But it is _possible_ if necessary -- is it?
            # output_schema=ctx.execution.bound_interface.resolve_nominal_output_schema(
            #     ctx.worker.env
            # ),
        )
        sql = compile_jinja_sql(parsed.sql_with_jinja_vars, sql_ctx)
        return sql

    def get_parsed_statement(self) -> ParsedSqlStatement:
        return parse_sql_statement(self.sql, self.autodetect_inputs)

    def get_interface(self) -> DeclaredSnapInterface:
        stmt = self.get_parsed_statement()
        return stmt.as_interface()


def process_sql(sql: str, file_path: str = None) -> str:
    if sql.endswith(".sql"):
        if not file_path:
            raise Exception(
                f"Must specify @SqlSnap(file=__file__) in order to load sql file {sql}"
            )
        sql = load_file(file_path, sql)
    return sql


def sql_snap_factory(
    name: str,
    sql: str = None,
    file: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    compatible_runtimes: str = None,  # TODO: engine support
    wrapper_cls: type = SqlSnapWrapper,
    autodetect_inputs: bool = True,
    **kwargs,  # TODO: explicit options
) -> _Snap:
    if not sql:
        raise ValueError("Must provide sql")
    sql = process_sql(sql, file)
    p = snap_factory(
        wrapper_cls(sql, autodetect_inputs=autodetect_inputs),
        name=name,
        module=module,
        compatible_runtimes=compatible_runtimes or "database",
        ignore_signature=True,  # For SQL, ignore signature if explicit inputs are provided
        **kwargs,
    )
    return p


sql_snap = sql_snap_factory


def sql_snap_decorator(
    sql_fn_or_snap: Union[_Snap, Callable] = None,
    file: str = None,
    autodetect_inputs: bool = True,
    **kwargs,
) -> Union[Callable, _Snap]:
    if sql_fn_or_snap is None:
        # handle bare decorator @SqlSnap
        return partial(
            sql_snap_decorator, file=file, autodetect_inputs=autodetect_inputs, **kwargs
        )
    if isinstance(sql_fn_or_snap, _Snap):
        sql = sql_fn_or_snap.snap_callable()
        sql = process_sql(sql, file)
        sql_fn_or_snap.snap_callable = SqlSnapWrapper(
            sql, autodetect_inputs=autodetect_inputs
        )
        # TODO / FIXME: this is dicey ... if we ever add / change args for snap_factory
        # will break this. (we're only taking a select few args from the exising Snap)
        return snap_factory(
            sql_fn_or_snap,
            ignore_signature=True,
            compatible_runtimes="database",
            module=sql_fn_or_snap.module_name,
            _original_object=sql_fn_or_snap.snap_callable,
            **kwargs,
        )
    sql = sql_fn_or_snap()
    if "name" in kwargs:
        name = kwargs.pop("name")
    else:
        name = sql_fn_or_snap.__name__
    return sql_snap_factory(
        name=name, sql=sql, file=file, autodetect_inputs=autodetect_inputs, **kwargs,
    )


SqlSnap = sql_snap_decorator
Sql = sql_snap_decorator

