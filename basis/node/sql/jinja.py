from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Optional

from basis.configuration.base import PydanticBase
from jinja2.sandbox import SandboxedEnvironment

from basis.graph.configured_node import (
    NodeInterface,
    InputDefinition,
    ParameterDefinition,
    OutputDefinition,
    PortType,
    ParameterType,
)
from basis.node.node import InputTable, Parameter


@dataclass
class BasisJinjaInspectContext:
    inputs: list[InputDefinition] = field(default_factory=list)
    outputs: list[OutputDefinition] = field(default_factory=list)
    parameters: list[ParameterDefinition] = field(default_factory=list)

    def input_table(self, name: str, description: str = None, schema: str = None):
        self.inputs.append(
            InputDefinition(
                name=name,
                port_type=PortType.Table,
                description=description,
                schema_or_name=schema,
                required=True,
            )
        )

    def output_table(self, name: str, description: str = None, schema: str = None):
        self.outputs.append(
            OutputDefinition(
                name=name,
                port_type=PortType.Table,
                description=description,
                schema_or_name=schema,
            )
        )

    def parameter(
        self, name: str, type: str = None, description: str = None, default: Any = None
    ):
        self.parameters.append(
            ParameterDefinition(
                name=name,
                parameter_type=ParameterType(type) if type else None,
                description=description,
                default=default,
            )
        )


def _jinja_ctx_from_inspect_ctx(ctx: BasisJinjaInspectContext) -> dict:
    return {
        "InputTable": ctx.input_table,
        "OutputTable": ctx.output_table,
        "Parameter": ctx.parameter,
    }


def _get_jinja_env() -> SandboxedEnvironment:
    return SandboxedEnvironment()


def _interface_from_jinja_ctx(ctx: BasisJinjaInspectContext) -> NodeInterface:
    return NodeInterface(
        inputs=ctx.inputs, outputs=ctx.outputs, parameters=ctx.parameters, state=None
    )


def parse_interface_from_sql(t: str, ctx: dict = None) -> NodeInterface:
    env = _get_jinja_env()
    ctx = ctx or {}
    basis_ctx = BasisJinjaInspectContext()
    ctx.update(_jinja_ctx_from_inspect_ctx(basis_ctx))
    env.from_string(t).render(ctx)
    return _interface_from_jinja_ctx(basis_ctx)
