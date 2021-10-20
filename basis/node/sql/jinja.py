from __future__ import annotations
from collections import OrderedDict

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from basis.configuration.base import PydanticBase
from basis.node.interface import (
    DEFAULT_OUTPUT_NAME,
    NodeInterface,
    IoBase,
    Parameter,
    RecordStream,
    Table,
)
from jinja2.sandbox import SandboxedEnvironment


class BasisJinjaInspectContext(PydanticBase):
    tables: List[IoBase] = []
    records: List[IoBase] = []
    parameters: List[Parameter] = []

    def record(self, *args, **kwargs):
        self.records.append(RecordStream(*args, **kwargs))

    def table(self, *args, **kwargs):
        self.tables.append(Table(*args, **kwargs))

    def parameter(self, *args, **kwargs):
        self.parameters.append(Parameter(*args, **kwargs))


class BasisJinjaRenderContext(PydanticBase):
    inputs_sql: Dict[str, str]
    params_sql: Dict[str, str]

    def render_input(self, name: str, *args, **kwargs):
        return self.inputs_sql[name]

    def render_parameter(self, name: str, *args, **kwargs):
        return self.params_sql[name]


def get_base_jinja_inspect_ctx() -> Dict[str, Any]:
    basis_ctx = BasisJinjaInspectContext()
    ctx = {
        "basis_ctx": basis_ctx,
        "Record": basis_ctx.record,
        "Table": basis_ctx.table,
        "Parameter": basis_ctx.parameter,
    }
    return ctx


def get_base_jinja_render_ctx(
    inputs_sql: Dict[str, str], params_sql: Dict[str, str]
) -> Dict[str, Any]:
    basis_ctx = BasisJinjaRenderContext(inputs_sql=inputs_sql, params_sql=params_sql)
    ctx = {
        "Record": basis_ctx.render_input,
        "Table": basis_ctx.render_input,
        "Parameter": basis_ctx.render_parameter,
    }
    return ctx


def get_jinja_env() -> SandboxedEnvironment:
    return SandboxedEnvironment()


def interface_from_jinja_ctx(ctx: Dict) -> NodeInterface:
    # TODO: multiple outputs?
    bc = ctx["basis_ctx"]
    inputs = []
    assert len(bc.records) <= 1, "More than one record stream"
    inputs.extend(bc.records)
    inputs.extend(bc.tables)
    return NodeInterface(
        inputs=OrderedDict({i.name: i for i in inputs}),
        outputs=OrderedDict({DEFAULT_OUTPUT_NAME: Table(DEFAULT_OUTPUT_NAME)}),
        parameters=OrderedDict({p.name: p for p in bc.parameters}),
    )


def parse_interface_from_sql(t: str, ctx: Optional[Dict] = None):
    env = get_jinja_env()
    ctx = ctx or {}
    ctx.update(get_base_jinja_inspect_ctx())
    env.from_string(t).render(ctx)
    return interface_from_jinja_ctx(ctx)


def render_sql(
    t: str,
    inputs_sql: Dict[str, str],
    params_sql: Dict[str, str],
    ctx: Optional[Dict] = None,
):
    env = get_jinja_env()
    ctx = ctx or {}
    ctx.update(get_base_jinja_render_ctx(inputs_sql, params_sql))
    return env.from_string(t).render(ctx)
