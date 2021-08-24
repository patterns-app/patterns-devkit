from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from basis.core.declarative.base import update
from basis.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    BlockType,
    FunctionInterfaceCfg,
    IoBaseCfg,
    Parameter,
    ParameterType,
    Table,
)
from basis.core.persistence.schema import is_generic
from basis.utils.docstring import BasisParser, Docstring
from commonmodel.base import SchemaLike

if TYPE_CHECKING:
    from basis.core.function import (
        InputExhaustedException,
        FunctionCallable,
    )


re_input_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)
re_output_type_hint = re.compile(
    r"(?P<iterator>(Iterator|Iterable|Generator)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)


def normalize_parameter_type(pt: str) -> str:
    return dict(text="str", boolean="bool", number="float", integer="int",).get(
        pt.lower(), pt
    )


@dataclass
class ParsedAnnotation:
    block_type: Optional[BlockType] = None
    parameter_type: Optional[ParameterType] = None
    output_type: Optional[str] = None
    schema: Optional[str] = None
    optional: bool = False
    iterator: bool = False
    original_annotation: Optional[str] = None
    is_context: bool = False
    is_none: bool = False


class BadAnnotationException(Exception):
    pass


def parse_input_annotation(a: str) -> ParsedAnnotation:
    parsed = ParsedAnnotation(original_annotation=a)
    m = re_input_type_hint.match(a)
    if m is None:
        raise BadAnnotationException(f"Invalid Function annotation '{a}'")
    md = m.groupdict()
    if md.get("optional"):
        parsed.optional = True
    origin = md["origin"]
    if origin in ("Context", "Context"):
        parsed.is_context = True
    elif origin in [it.value for it in BlockType]:
        parsed.block_type = BlockType(origin)
        parsed.schema = md.get("arg")
    elif normalize_parameter_type(origin) in [pt.value for pt in ParameterType]:
        parsed.parameter_type = ParameterType(normalize_parameter_type(origin))
    else:
        raise BadAnnotationException(
            f"{origin} is not an accepted input or parameter type"
        )
    return parsed


def parse_output_annotation(a: str) -> ParsedAnnotation:
    parsed = ParsedAnnotation(original_annotation=a)
    m = re_output_type_hint.match(a)
    if m is None:
        raise BadAnnotationException(f"Invalid Function annotation '{a}'")
    md = m.groupdict()
    if md.get("iterator"):
        parsed.iterator = True
    origin = md["origin"]
    if origin == "None":
        parsed.is_none = True
    else:
        parsed.output_type = (
            origin  # TODO: validate this type? hard with any possible format
        )
    parsed.schema = md.get("arg")
    return parsed


def parse_docstring(d: str) -> Docstring:
    return BasisParser().parse(d)


DEFAULT_INPUT_ANNOTATION = "Block"
DEFAULT_OUTPUT = Table(name=DEFAULT_OUTPUT_NAME,)
DEFAULT_OUTPUTS = {DEFAULT_OUTPUT_NAME: DEFAULT_OUTPUT}
DEFAULT_STATE_OUTPUT_NAME = "state"
DEFAULT_STATE_OUTPUT = None  # IoBase(
#     schema="core.State", name=DEFAULT_STATE_OUTPUT_NAME, is_default=False
# )
DEFAULT_ERROR_OUTPUT_NAME = "error"
DEFAULT_ERROR_OUTPUT = None  # IoBase(
#     schema="Any",  # TODO: probably same as default output (how to parameterize tho?)
#     name=DEFAULT_ERROR_OUTPUT_NAME,
#     is_default=False,
# )


class NonStringAnnotationException(Exception):
    def __init__(self, obj):
        super().__init__(
            f"Type annotation must be a string (got `{obj}`). Try adding "
            "`from __future__ import annotations` to file where function is declared."
        )


def function_interface_from_callable(
    function: FunctionCallable,
) -> FunctionInterfaceCfg:
    signature = inspect.signature(function)
    outputs = {}
    inputs = {}
    params = {}
    uses_context = False
    unannotated_found = False
    # Output
    ret = signature.return_annotation
    if ret is inspect.Signature.empty:
        """
        By default, we assume every Function may return an output,
        unless specifically annotated with '-> None'.
        """
        default_output = DEFAULT_OUTPUT
    else:
        if not isinstance(ret, str):
            raise NonStringAnnotationException(ret)
        default_output = function_output_from_annotation(ret)
    if default_output is not None:
        outputs[DEFAULT_OUTPUT_NAME] = default_output
    # Inputs and params
    parameters = list(signature.parameters.items())
    for name, param in parameters:
        a = param.annotation
        annotation_is_empty = a is inspect.Signature.empty
        if annotation_is_empty:
            if name in ("ctx", "context"):
                uses_context = True
                continue
            else:
                # TODO: handle un-annotated parameters?
                if unannotated_found:
                    raise Exception(
                        "Cannot have more than one un-annotated input to a Function"
                    )
                unannotated_found = True
                a = DEFAULT_INPUT_ANNOTATION
        if not annotation_is_empty and not isinstance(a, str):
            if isinstance(a, type):
                a = a.__name__
            else:
                raise NonStringAnnotationException(a)
        parsed = parse_input_annotation(a)
        if parsed.is_context:
            uses_context = True
        elif parsed.input_type is None:
            assert parsed.parameter_type is not None
            default = None
            if param.default is not inspect.Signature.empty:
                default = param.default
            p = parameter_from_annotation(parsed, name=name, default=default)
            params[p.name] = p
        else:
            i = function_input_from_annotation(parsed, name=param.name,)
            inputs[i.name] = i
    cfg = FunctionInterfaceCfg(
        inputs=inputs, outputs=outputs, parameters=params, uses_context=uses_context
    )
    if function.__doc__:
        cfg = update_interface_with_docstring(cfg, function.__doc__)
    return cfg


def update_interface_with_docstring(
    cfg: FunctionInterfaceCfg, doc: str
) -> FunctionInterfaceCfg:
    docstring = parse_docstring(doc)
    for p in docstring.params:
        if p.arg_name in cfg.parameters:
            cfg.parameters[p.arg_name] = update(
                cfg.parameters[p.arg_name], description=p.description
            )
    for i in docstring.inputs:
        if i.input_name in cfg.inputs:
            cfg.inputs[i.input_name] = update(
                cfg.inputs[i.input_name], description=i.description
            )
    # if docstring.returns:
    #     stdout = cfg.get_default_output()
    #     stdout.description = update(docstring.returns.description)

    # TODO: this belongs on data function itself?
    # if docstring.short_description:
    #     cfg = update(cfg, description=docstring.short_description)
    return cfg


def function_input_from_parameter(param: inspect.Parameter) -> FunctionInputCfg:
    a = param.annotation
    annotation_is_empty = a is inspect.Signature.empty
    if not annotation_is_empty and not isinstance(a, str):
        raise NonStringAnnotationException()
    annotation = param.annotation
    if annotation is inspect.Signature.empty:
        if param.name in ("ctx", "context"):  # TODO: hack
            annotation = "Context"
        else:
            annotation = DEFAULT_INPUT_ANNOTATION
    # is_optional = param.default != inspect.Parameter.empty
    parsed = parse_input_annotation(annotation)
    return function_input_from_annotation(parsed, name=param.name,)


def function_input_from_annotation(
    parsed: ParsedAnnotation, name: str
) -> FunctionInputCfg:
    return FunctionInputCfg(
        name=name,
        schema_key=parsed.schema or "Any",
        input_type=parsed.input_type or InputType.Block,
        required=(not parsed.optional)
        and (parsed.input_type != InputType.SelfReference),
    )


def function_output_from_annotation(
    annotation: Union[str, ParsedAnnotation]
) -> Optional[FunctionOutputCfg]:
    if isinstance(annotation, str):
        parsed = parse_output_annotation(annotation)
    else:
        parsed = annotation
    if parsed.is_none:
        return None
    return FunctionOutputCfg(
        schema_key=parsed.schema or "Any",
        data_format=parsed.output_type,
        is_iterator=parsed.iterator,
    )


def parameter_from_annotation(
    parsed: ParsedAnnotation, name: str, default: Any, description: str = None
) -> Parameter:
    return Parameter(
        name=name,
        datatype=(
            parsed.parameter_type or ParameterType.Text
        ).value,  # TODO: standardize param datatype to enum
        required=(not parsed.optional),
        default=default,
        description=description or "",
    )
