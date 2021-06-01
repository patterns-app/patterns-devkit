from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from commonmodel.base import SchemaLike
from snapflow.core.data_block import Consumable, DataBlock, Reference, SelfReference
from snapflow.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
    DataFunctionOutputCfg,
    InputType,
    Parameter,
    ParameterType,
)
from snapflow.core.schema import is_generic

if TYPE_CHECKING:
    from snapflow.core.function import (
        InputExhaustedException,
        DataFunctionCallable,
    )


re_input_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)
re_output_type_hint = re.compile(
    r"(?P<iterator>(Iterator|Iterable|Generator)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)


def normalize_parameter_type(pt: str) -> str:
    return dict(
        text="str",
        boolean="bool",
        number="float",
        integer="int",
    ).get(pt, pt)


@dataclass
class ParsedAnnotation:
    input_type: Optional[InputType] = None
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
        raise BadAnnotationException(f"Invalid DataFunction annotation '{a}'")
    md = m.groupdict()
    if md.get("optional"):
        parsed.optional = True
    origin = md["origin"]
    if origin in ("DataFunctionContext", "Context"):
        parsed.is_context = True
    elif origin in [it.value for it in InputType]:
        parsed.input_type = InputType(origin)
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
        raise BadAnnotationException(f"Invalid DataFunction annotation '{a}'")
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


DEFAULT_INPUT_ANNOTATION = "DataBlock"
DEFAULT_OUTPUT = DataFunctionOutputCfg(
    schema_key="Any",
    name=DEFAULT_OUTPUT_NAME,
)
DEFAULT_OUTPUTS = {DEFAULT_OUTPUT_NAME: DEFAULT_OUTPUT}
DEFAULT_STATE_OUTPUT_NAME = "state"
DEFAULT_STATE_OUTPUT = DataFunctionOutputCfg(
    schema_key="core.State", name=DEFAULT_STATE_OUTPUT_NAME, is_default=False
)
DEFAULT_ERROR_OUTPUT_NAME = "error"
DEFAULT_ERROR_OUTPUT = DataFunctionOutputCfg(
    schema_key="Any",  # TODO: probably same as default output (how to parameterize tho?)
    name=DEFAULT_ERROR_OUTPUT_NAME,
    is_default=False,
)


class NonStringAnnotationException(Exception):
    def __init__(self):
        super().__init__(
            "Parameter type annotation must be a string. Try adding "
            "`from __future__ import annotations` to file where function is declared."
        )


def function_interface_from_callable(
    function: DataFunctionCallable,
) -> DataFunctionInterfaceCfg:
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
        By default, we assume every DataFunction may return an output,
        unless specifically annotated with '-> None'.
        """
        default_output = DEFAULT_OUTPUT
    else:
        if not isinstance(ret, str):
            raise NonStringAnnotationException()
        default_output = function_output_from_annotation(ret)
    if default_output is not None:
        outputs[DEFAULT_OUTPUT_NAME] = default_output
    # Inputs and params
    parameters = list(signature.parameters.items())
    for name, param in parameters:
        a = param.annotation
        annotation_is_empty = a is inspect.Signature.empty
        if not annotation_is_empty and not isinstance(a, str):
            raise NonStringAnnotationException()
        if annotation_is_empty:
            if name in ("ctx", "context"):
                uses_context = True
                continue
            else:
                # TODO: handle un-annotated parameters?
                if unannotated_found:
                    raise Exception(
                        "Cannot have more than one un-annotated input to a DataFunction"
                    )
                unannotated_found = True
                a = DEFAULT_INPUT_ANNOTATION
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
            i = function_input_from_annotation(
                parsed,
                name=param.name,
            )
            inputs[i.name] = i
    return DataFunctionInterfaceCfg(
        inputs=inputs, outputs=outputs, parameters=params, uses_context=uses_context
    )


def function_input_from_parameter(param: inspect.Parameter) -> DataFunctionInputCfg:
    a = param.annotation
    annotation_is_empty = a is inspect.Signature.empty
    if not annotation_is_empty and not isinstance(a, str):
        raise NonStringAnnotationException()
    annotation = param.annotation
    if annotation is inspect.Signature.empty:
        if param.name in ("ctx", "context"):  # TODO: hack
            annotation = "DataFunctionContext"
        else:
            annotation = DEFAULT_INPUT_ANNOTATION
    # is_optional = param.default != inspect.Parameter.empty
    parsed = parse_input_annotation(annotation)
    return function_input_from_annotation(
        parsed,
        name=param.name,
    )


def function_input_from_annotation(
    parsed: ParsedAnnotation, name: str
) -> DataFunctionInputCfg:
    return DataFunctionInputCfg(
        name=name,
        schema_key=parsed.schema or "Any",
        input_type=parsed.input_type or InputType.DataBlock,
        required=(not parsed.optional)
        and (parsed.input_type != InputType.SelfReference),
    )


def function_output_from_annotation(
    annotation: Union[str, ParsedAnnotation]
) -> Optional[DataFunctionOutputCfg]:
    if isinstance(annotation, str):
        parsed = parse_output_annotation(annotation)
    else:
        parsed = annotation
    if parsed.is_none:
        return None
    return DataFunctionOutputCfg(
        schema_key=parsed.schema or "Any",
        data_format=parsed.output_type,
        is_iterator=parsed.iterator,
    )


def parameter_from_annotation(
    parsed: ParsedAnnotation, name: str, default: Any, help: str = None
) -> Parameter:
    return Parameter(
        name=name,
        datatype=(
            parsed.parameter_type or ParameterType.Text
        ).value,  # TODO: standardize param datatype to enum
        required=(not parsed.optional),
        default=default,
        help=help or "",
    )
