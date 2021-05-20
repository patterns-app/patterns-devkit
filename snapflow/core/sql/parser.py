from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from jinja2 import TemplateSyntaxError, nodes
from jinja2.ext import Extension
from jinja2.nodes import Const
from jinja2.runtime import Undefined
from jinja2.sandbox import SandboxedEnvironment
from snapflow.core.declarative.function import DataFunctionInterfaceCfg
from snapflow.core.function_interface import (
    DEFAULT_OUTPUTS,
    InputType,
    ParsedAnnotation,
    function_input_from_annotation,
    parameter_from_annotation,
    parse_input_annotation,
)


class NamedGetItem:
    def __init__(self, name: str):
        self.name = name

    def __getitem__(self, name: str) -> str:
        return f"{self.name}[{name}]"


class NodeInputExtension(Extension):
    tags = {"input"}

    def __init__(self, environment):
        super().__init__(environment)
        environment.extend(node_inputs=[])
        environment.extend(node_renderer=None)

    def parse(self, parser):
        line_number = next(parser.stream).lineno
        input_name = parser.parse_expression()
        try:
            annotation = parser.parse_expression()
        except TemplateSyntaxError:
            annotation = Const(None)
        return nodes.CallBlock(
            self.call_method("_log_input", [input_name, annotation]), [], [], ""
        ).set_lineno(line_number)

    def _log_input(self, *args, caller=None):
        # str_args = [str(a) for a in args]
        self.environment.node_inputs.append(args)
        if self.environment.node_renderer:
            return self.environment.node_renderer(*args)
        return f"InputNode({', '.join([str(a) for a in args])})"


class ParamExtension(Extension):
    tags = {"param"}

    def __init__(self, environment):
        super().__init__(environment)
        environment.extend(params=[])
        environment.extend(param_renderer=None)

    def parse(self, parser):
        line_number = next(parser.stream).lineno
        param_name = parser.parse_expression()
        try:
            dtype = parser.parse_expression()
            # if isinstance(dtype, NamedGetItem):
            #     dtype = dtype.render()
        except TemplateSyntaxError:
            dtype = Const(None)
        try:
            default = parser.parse_expression()
        except TemplateSyntaxError:
            default = Const(None)
        try:
            help_text = parser.parse_expression()
        except TemplateSyntaxError:
            help_text = Const("")
        return nodes.CallBlock(
            self.call_method("_log_params", [param_name, dtype, default, help_text]),
            [],
            [],
            "",
        ).set_lineno(line_number)

    def _log_params(self, *args, caller=None):
        # str_args = [str(a) for a in args]
        self.environment.params.append(args)
        if self.environment.param_renderer:
            return self.environment.param_renderer(*args)
        return f"Parameter({', '.join([str(a) for a in args])})"


class StringUndefined(Undefined):
    def __str__(self):
        return self._undefined_name

    def __repr__(self):
        return str(self)


DFEAULT_PARAMETER_ANNOTATION = "text"
DEFAULT_SQL_INPUT_ANNOTATION = "Reference"


def parse_sql_annotation(ann: str) -> ParsedAnnotation:
    if "[" in ann or ann in (i.value for i in InputType):
        # If type annotation is complex or a InputType, parse it
        parsed = parse_input_annotation(ann)
    else:
        # If it's just a simple word, then assume it is a Schema name
        parsed = ParsedAnnotation(schema=ann, input_type=InputType.Reference)
    return parsed


def interface_from_jinja_env(env: SandboxedEnvironment) -> DataFunctionInterfaceCfg:
    inputs = {}
    outputs = {}
    params = {}
    for name, annotation in env.node_inputs:
        name = str(name)
        if annotation:
            annotation = str(annotation)
        ann = parse_sql_annotation(annotation or DEFAULT_SQL_INPUT_ANNOTATION)
        inpt = function_input_from_annotation(ann, name=name)
        inputs[name] = inpt
    # TODO: output
    # if self.output_annotation:
    #     output = function_output_from_annotation(
    #         parse_sql_annotation(self.output_annotation)
    #     )
    #     if output:
    #         outputs = {DEFAULT_OUTPUT_NAME: output}
    # else:
    #     outputs = DEFAULT_OUTPUTS
    outputs = DEFAULT_OUTPUTS
    for name, dtype, default, help_text in env.params:
        name = str(name)
        if dtype:
            dtype = str(dtype)
        if default:
            default = str(default)
        if help_text:
            help_text = str(help_text)
        params[name] = parameter_from_annotation(
            parse_input_annotation(dtype or DFEAULT_PARAMETER_ANNOTATION),
            name=name,
            default=default,
            help=help_text,
        )

    return DataFunctionInterfaceCfg(
        inputs=inputs, outputs=outputs, uses_context=True, parameters=params
    )


def get_base_jinja_ctx() -> Dict[str, Any]:
    ctx = dict(
        Optional=NamedGetItem("Optional"),
    )
    for t in InputType:
        ctx[t.value] = NamedGetItem(t.value)
    return ctx


def get_jinja_env() -> SandboxedEnvironment:
    return SandboxedEnvironment(
        undefined=StringUndefined,
        extensions=[NodeInputExtension, ParamExtension],
    )


def parse_interface_from_sql(t: str, ctx: Optional[Dict] = None):
    env = get_jinja_env()
    ctx = ctx or {}
    ctx.update(get_base_jinja_ctx())
    env.from_string(t).render(ctx)
    return interface_from_jinja_env(env)


class LookupRenderer:
    def __init__(self, lookup: Dict[str, str]):
        self.lookup = lookup

    def __call__(self, name: str, *args) -> str:
        return self.lookup.get(str(name).strip(), "")


def render_sql(
    t: str,
    inputs_sql: Dict[str, str],
    params_sql: Dict[str, str],
    ctx: Optional[Dict] = None,
):
    env = get_jinja_env()
    ctx = ctx or {}
    ctx.update(get_base_jinja_ctx())
    env.node_renderer = LookupRenderer(inputs_sql)
    env.param_renderer = LookupRenderer(params_sql)
    return env.from_string(t).render(ctx)


s = "select * from {% input orders %} where col = {% param p1 text 0 %}"
parse_interface_from_sql(s)

render_sql(
    s,
    dict(orders="orders_table"),
    dict(p1="'val1'"),
)
