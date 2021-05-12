from dataclasses import dataclass
from typing import Dict, Optional
from jinja2 import nodes, TemplateSyntaxError
from jinja2.ext import Extension
from jinja2.nodes import Const
from jinja2.runtime import Undefined
from jinja2.sandbox import SandboxedEnvironment
from snapflow.core.function_interface import (
    DEFAULT_OUTPUTS,
    DataFunctionInterface,
    InputType,
    ParsedAnnotation,
    function_input_from_annotation,
    parameter_from_annotation,
    parse_input_annotation,
)


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
        environment.extend(params_renderer=None)

    def parse(self, parser):
        line_number = next(parser.stream).lineno
        param_name = parser.parse_expression()
        try:
            dtype = parser.parse_expression()
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
        if self.environment.params_renderer:
            return self.environment.params_renderer(*args)
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


def interface_from_jinja_env(env: SandboxedEnvironment) -> DataFunctionInterface:
    inputs = {}
    outputs = {}
    params = {}
    for name, annotation in env.node_inputs:
        print(name, annotation)
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
        print(name, dtype, default)
        dtype = str(dtype)
        params[name] = parameter_from_annotation(
            parse_input_annotation(dtype or DFEAULT_PARAMETER_ANNOTATION),
            name=name,
            default=default,
            help=help_text,
        )

    return DataFunctionInterface(
        inputs=inputs, outputs=outputs, uses_context=True, parameters=params
    )


def get_jinja_env() -> SandboxedEnvironment:
    return SandboxedEnvironment(
        undefined=StringUndefined, extensions=[NodeInputExtension, ParamExtension],
    )


def parse_interface_from_sql(t: str, ctx: Optional[Dict] = None):
    env = get_jinja_env()
    env.from_string(t).render(ctx or {})
    return interface_from_jinja_env(env)


parse_interface_from_sql(
    "select * from  {% input orders ShopifyOrder %} where col = {% param p1 text 0 %}"
)

