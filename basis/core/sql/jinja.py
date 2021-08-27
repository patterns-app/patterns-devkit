from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from basis.core.declarative.base import PydanticBase
from basis.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    FunctionInterfaceCfg,
    IoBaseCfg,
    Parameter,
    ParameterCfg,
    Record,
    Table,
)
from jinja2.sandbox import SandboxedEnvironment


class BasisJinjaInspectContext(PydanticBase):
    tables: List[IoBaseCfg] = []
    records: List[IoBaseCfg] = []
    parameters: List[ParameterCfg] = []

    def record(self, *args, **kwargs):
        self.records.append(Record(*args, **kwargs))

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


def interface_from_jinja_ctx(ctx: Dict) -> FunctionInterfaceCfg:
    bc = ctx["basis_ctx"]
    inputs = []
    assert len(bc.records) <= 1
    inputs.extend(bc.records)
    inputs.extend(bc.tables)
    return FunctionInterfaceCfg(
        inputs={i.name: i for i in inputs},
        outputs={DEFAULT_OUTPUT_NAME: Table(DEFAULT_OUTPUT_NAME)},
        parameters={p.name: p for p in bc.parameters},
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


# s = "select * from {% input orders %} where col = {% param p1 text 0 %}"
# parse_interface_from_sql(s)

# render_sql(
#     s, dict(orders="orders_table"), dict(p1="'val1'"),
# )
