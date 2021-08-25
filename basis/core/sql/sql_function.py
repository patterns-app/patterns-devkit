from __future__ import annotations
from basis.core.declarative.interface import resolve_nominal_output_schema

import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import sqlparse
from basis.core.component import ComponentLibrary
from basis.core.block import Block
from basis.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    FunctionInterfaceCfg,
    Table,
)
from basis.core.environment import Environment
from basis.core.execution.context import Context
from basis.core.function import (
    Function,
    DataInterfaceType,
    function_decorator,
    make_function_from_bare_callable,
)

from basis.core.function_package import load_file
from basis.core.sql.jinja import parse_interface_from_sql, render_sql
from commonmodel.base import SchemaLike, SchemaTranslation
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass, Storage
from dcp.storage.database.utils import column_map, compile_jinja_sql
from dcp.utils.common import rand_str
from loguru import logger
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


@dataclass(frozen=True)
class ParsedSqlStatement:
    original_sql: str
    sql_with_jinja_vars: str
    found_tables: Optional[Dict[str, str]] = None
    interface: Optional[FunctionInterfaceCfg] = None


def regex_repalce_match(s, m, r) -> str:
    return s[: m.start()] + r + s[m.end() :]


@dataclass
class TableParseState:
    prev_token: Optional[str] = None
    table_identifier_stmt: bool = False
    table_identifier_required_next: bool = False
    with_identifier_required_next: bool = False
    jinja_context_cnt: int = 0


def jinja_table_ref(table_name: str) -> str:
    return '{{ Table("%s") }} as %s' % (table_name, table_name)


def extract_tables(  # noqa: C901
    sql: str, replace_with_inputs_jinja: bool = True
) -> ParsedSqlStatement:
    """
    Extract tables that have no annotation.
    Table aliases MUST use explicit `as`
    """
    # TODO: still breaks with multiple CTEs...
    # `with cte1 as (...), cte2 as (....)`  <-- won't catch cte2 (requires full grammar parse)
    # so will consider a subsequent `from cte2 ...` as an input reference
    # probably fixable by just assuming all `, <ident> as (` are potential ctes
    found_tables = {}
    with_aliases = set()
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
            if t == "(":
                # Handle subquery
                if state.table_identifier_stmt:
                    state.table_identifier_required_next = False
                    state.table_identifier_stmt = False
                    continue
            if token.is_keyword:
                if t == "with":
                    state.with_identifier_required_next = True
                    continue
                else:
                    state.with_identifier_required_next = False
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
                if state.with_identifier_required_next:
                    with_alias = str(token)
                    with_aliases.add(with_alias)
                    state.with_identifier_required_next = False
                    continue
                if state.table_identifier_stmt:
                    table_ref = str(token)
                    if table_ref not in with_aliases:
                        # Only if it's not a with alias
                        found_tables[table_ref] = table_ref  # TODO
                        if replace_with_inputs_jinja:
                            new_sql.pop()
                            new_sql.append(jinja_table_ref(table_ref))
                    state.table_identifier_required_next = False
    new_sql_str = "".join(new_sql)
    new_sql_str = re.sub(r"as\s+\w+\s+as\s+(\w+)", r"as \1", new_sql_str, flags=re.I)
    return ParsedSqlStatement(
        original_sql=sql, sql_with_jinja_vars=new_sql_str, found_tables=found_tables,
    )


def get_interface_from_sql(
    sql: str, autodetect_tables: bool = True
) -> ParsedSqlStatement:
    interface = parse_interface_from_sql(sql)
    new_sql = sql
    if not interface.inputs and autodetect_tables:
        # We ONLY autodetect non-jinja inputs if no explicit jinja inputs
        # TODO: still breaks with multiple CTEs...
        table_parse = extract_tables(sql)
        new_sql = table_parse.sql_with_jinja_vars
        for table in table_parse.found_tables or []:
            interface.inputs[table] = Table(name=table)
    return ParsedSqlStatement(
        original_sql=sql, sql_with_jinja_vars=new_sql, interface=interface,
    )


def params_as_sql(ctx: Context) -> Dict[str, Any]:
    param_values = ctx.get_params()
    sql_params = {}
    for k, v in param_values.items():
        p = ctx.function.get_param(k)
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
    lib: ComponentLibrary,
    name: str,
    translation: SchemaTranslation,
    quote_identifier: Callable = None,
) -> str:
    if not translation.from_schema_key:
        raise NotImplementedError(
            f"Schema translation must provide `from_schema` when translating a database table {translation}"
        )
    sql = column_map(
        name,
        lib.get_schema(translation.from_schema_key).field_names(),
        translation.as_dict(),
        quote_identifier=quote_identifier,
    )
    table_stmt = f"""
        (
            {sql}
        ) as __translated
        """
    return table_stmt


def emit_table_from_sql(
    ctx: Context, sql: str, storage: Storage, nominal_output_schema: SchemaLike = None
):
    db_api = storage.get_api()
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
    ctx.emit_table(
        table_name=tmp_name,
        storage=storage,
        data_format=DatabaseTableFormat,
        schema=nominal_output_schema,
    )


class SqlFunctionWrapper:
    def __init__(self, sql: str, autodetect_inputs: bool = True):
        self.sql = sql
        self.autodetect_inputs = autodetect_inputs

    def __call__(self, ctx: Context):
        # TODO: way to specify more granular storage requirements (engine, engine version, etc)
        storage = ctx.execution_config.get_target_storage()
        for storage in [
            ctx.execution_config.target_storage
        ] + ctx.execution_config.storages:
            storage = Storage(storage)
            if storage.storage_engine.storage_class == DatabaseStorageClass:
                break
        else:
            raise Exception("No database storage found, cannot execute function sql")

        sql = self.get_compiled_sql(ctx, storage)

        resolved_nominal_key = resolve_nominal_output_schema(
            self.get_interface(), ctx.inputs
        )
        emit_table_from_sql(ctx, sql, storage, resolved_nominal_key)

    def get_input_table_stmts(
        self, ctx: Context, storage: Storage, inputs: Dict[str, Block] = None,
    ) -> Dict[str, str]:
        if inputs is None:
            return {}
        table_stmts = {}
        for input_name, block in inputs.items():
            if isinstance(block, Block):
                table_stmts[input_name] = block.as_sql_from_stmt(storage)
        return table_stmts

    def get_compiled_sql(
        self, ctx: Context, storage: Storage, inputs: Dict[str, Block] = None,
    ):
        input_sql = self.get_input_table_stmts(ctx, storage, inputs)
        sql_ctx = dict(
            ctx=ctx,
            inputs=input_sql,  # TODO: change this (??)
            input_objects={n: i for n, i in ctx.inputs.items()},
            params=params_as_sql(ctx),
            storage=storage,
            # TODO: we haven't logged the input blocks yet (in the case of a stream) so we can't
            #    resolve the nominal output schema at this point. But it is _possible_ if necessary -- is it?
            # output_schema=ctx.execution.bound_interface.resolve_nominal_output_schema(
            #     ctx.worker.env
            # ),
        )
        return render_sql(self.sql, input_sql, params_as_sql(ctx), sql_ctx)
        # else:
        #     parsed = self.get_parsed_statement()
        #     sql = compile_jinja_sql(parsed.sql_with_jinja_vars, sql_ctx)
        #     return sql

    def get_parsed_statement(self) -> ParsedSqlStatement:
        return get_interface_from_sql(self.sql, self.autodetect_inputs)

    def get_interface(self) -> FunctionInterfaceCfg:
        return get_interface_from_sql(self.sql, self.autodetect_inputs).interface


def process_sql(sql: str, file_path: str = None) -> str:
    if sql.endswith(".sql"):
        if not file_path:
            raise Exception(
                f"Must specify @sql_function(file=__file__) in order to load sql file {sql}"
            )
        dir_path = Path(file_path) / ".."
        sql = load_file(str(dir_path), sql)
    return sql


def sql_function_factory(
    name: str,
    sql: str = None,
    file: str = None,
    required_storage_classes: List[str] = None,
    wrapper_cls: type = SqlFunctionWrapper,
    autodetect_inputs: bool = True,
    **kwargs,  # TODO: explicit options
) -> Function:
    if not sql:
        raise ValueError("Must provide sql")
    sql = process_sql(sql, file)
    p = make_function_from_bare_callable(
        wrapper_cls(sql, autodetect_inputs=autodetect_inputs),
        name=name,
        required_storage_classes=required_storage_classes or ["database"],
        **kwargs,
    )
    return p


sql_function = sql_function_factory


def sql_function_decorator(
    sql_fn_or_function: Union[Function, Callable] = None,
    file: str = None,
    autodetect_inputs: bool = True,
    **kwargs,
) -> Union[Callable, Function]:
    if sql_fn_or_function is None:
        # handle bare decorator @sql_function
        return partial(
            sql_function_decorator,
            file=file,
            autodetect_inputs=autodetect_inputs,
            **kwargs,
        )
    if isinstance(sql_fn_or_function, Function):
        sql = sql_fn_or_function.function_callable()
        sql = process_sql(sql, file)
        sql_fn_or_function.function_callable = SqlFunctionWrapper(
            sql, autodetect_inputs=autodetect_inputs
        )
        # TODO / FIXME: this is dicey ... if we ever add / change args for function_factory
        # will break this. (we're only taking a select few args from the exising Function)
        return make_function_from_bare_callable(
            sql_fn_or_function,
            required_storage_classes=["database"],
            # _original_object=sql_fn_or_function.function_callable,
            **kwargs,
        )
    # Else empty
    sql = sql_fn_or_function()
    if "name" in kwargs:
        name = kwargs.pop("name")
    else:
        name = sql_fn_or_function.__name__
    return sql_function_factory(
        name=name, sql=sql, file=file, autodetect_inputs=autodetect_inputs, **kwargs,
    )


sqlfunction = sql_function_decorator
