from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from commonmodel.base import SchemaLike
from snapflow.core.data_block import DataBlock
from snapflow.core.schema import is_generic

if TYPE_CHECKING:
    from snapflow.core.function import (
        InputExhaustedException,
        DataFunctionCallable,
    )
    from snapflow.core.node import Node, Node, NodeLike

    from snapflow.core.execution.executable import Executable
    from snapflow.core.streams import StreamLike


re_input_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)
re_output_type_hint = re.compile(
    r"(?P<iterator>(Iterator|Iterable|Generator)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)

DEFAULT_OUTPUT_NAME = "default"


# VALID_DATA_INTERFACE_TYPES = [
#     # TODO: is this list just a list of formats? which ones are valid i/o to DataFunctions?
#     # TODO: do we even want this check?
#     "Stream",
#     "DataBlockStream",
#     "Block",
#     "DataBlock",
#     "DataFrame",
#     "Records",
#     "RecordsIterator",
#     "DataFrameIterator",
#     "DatabaseTableRef",
#     "DataRecordsObject",
#     "Any",
#     # "DatabaseCursor",
# ]

# SELF_REF_PARAM_NAME = "this" # TODO: not necessary?


class InputType(Enum):
    DataBlock = "DataBlock"
    Reference = "Reference"
    Stream = "Stream"
    SelfReference = "SelfReference"
    Consumable = "Consumable"


# Type aliases
SelfReference = Union[DataBlock, None]
Reference = DataBlock
Consumable = DataBlock


class ParameterType(Enum):
    Text = "str"
    Boolean = "bool"
    Integer = "int"
    Float = "float"
    Date = "date"
    DateTime = "datetime"
    Json = "Dict"
    List = "List"


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
    elif origin in [pt.value for pt in ParameterType]:
        parsed.parameter_type = ParameterType(origin)
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


@dataclass
class Parameter:
    name: str
    datatype: str
    required: bool = False
    default: Any = None
    help: str = ""


@dataclass(frozen=True)
class DataFunctionInput:
    name: str
    schema_like: SchemaLike
    input_type: InputType = InputType.DataBlock
    required: bool = True
    description: Optional[str] = None

    @property
    def is_generic(self) -> bool:
        return is_generic(self.schema_like)

    @property
    def is_stream(self) -> bool:
        return self.input_type == InputType.Stream

    @property
    def is_self_reference(self) -> bool:
        return self.input_type == InputType.SelfReference

    @property
    def is_reference(self) -> bool:
        return self.input_type in (InputType.Reference, InputType.SelfReference)


@dataclass(frozen=True)
class DataFunctionOutput:
    schema_like: SchemaLike
    name: str = DEFAULT_OUTPUT_NAME
    data_format: Optional[Any] = None
    # reference: bool = False # TODO: not a thing right? that's up to downstream to decide
    # optional: bool = False
    is_iterator: bool = False
    is_default: bool = True
    # stream: bool = False # TODO: do we ever need this?
    description: Optional[str] = None

    @property
    def is_generic(self) -> bool:
        return is_generic(self.schema_like)


DEFAULT_INPUT_ANNOTATION = "DataBlock"
DEFAULT_OUTPUT = DataFunctionOutput(schema_like="Any", name=DEFAULT_OUTPUT_NAME,)
DEFAULT_OUTPUTS = {DEFAULT_OUTPUT_NAME: DEFAULT_OUTPUT}
DEFAULT_STATE_OUTPUT_NAME = "state"
DEFAULT_STATE_OUTPUT = DataFunctionOutput(
    schema_like="core.State", name=DEFAULT_STATE_OUTPUT_NAME, is_default=False
)
DEFAULT_ERROR_OUTPUT_NAME = "error"
DEFAULT_ERROR_OUTPUT = DataFunctionOutput(
    schema_like="Any",  # TODO: probably same as default output (how to parameterize tho?)
    name=DEFAULT_ERROR_OUTPUT_NAME,
    is_default=False,
)


@dataclass(frozen=True)
class DataFunctionInterface:
    inputs: Dict[str, DataFunctionInput]
    outputs: Dict[str, DataFunctionOutput]
    parameters: Dict[str, Parameter]
    uses_context: bool = False
    uses_state: bool = False
    uses_error: bool = False
    none_output: bool = (
        False  # When return explicitly set to None (for identifying exporters) #TODO
    )

    def get_input(self, name: str) -> DataFunctionInput:
        return self.inputs[name]

    def get_single_input(self) -> DataFunctionInput:
        assert len(self.inputs) == 1, self.inputs
        return self.inputs[list(self.inputs)[0]]

    def get_single_non_recursive_input(self) -> DataFunctionInput:
        inpts = self.get_non_recursive_inputs()
        assert len(inpts) == 1, inpts
        return inpts[list(inpts)[0]]

    def get_non_recursive_inputs(self) -> Dict[str, DataFunctionInput]:
        return {
            n: i
            for n, i in self.inputs.items()
            if not i.input_type == InputType.SelfReference
        }

    # TODO: move these assign methods somewhere else?
    def assign_inputs(
        self, inputs: Union[StreamLike, Dict[str, StreamLike]]
    ) -> Dict[str, StreamLike]:
        if not isinstance(inputs, dict):
            assert (
                len(self.get_non_recursive_inputs()) == 1
            ), f"Wrong number of inputs. (Variadic inputs not supported yet) {inputs} {self.get_non_recursive_inputs()}"
            return {self.get_single_non_recursive_input().name: inputs}
        input_names_have = set(inputs.keys())
        input_names_must_have = set(
            i.name for i in self.get_non_recursive_inputs().values() if i.required
        )
        input_names_ok_to_have = set(self.inputs)
        assert (
            input_names_have >= input_names_must_have
        ), f"Missing required input(s): {input_names_must_have - input_names_have}"
        assert (
            input_names_have <= input_names_ok_to_have
        ), f"Extra input(s): {input_names_have - input_names_ok_to_have}"
        return inputs

    def assign_translations(
        self,
        declared_schema_translation: Optional[Dict[str, Union[Dict[str, str], str]]],
    ) -> Optional[Dict[str, Dict[str, str]]]:
        if not declared_schema_translation:
            return None
        v = list(declared_schema_translation.values())[0]
        if isinstance(v, str):
            # Just one translation, so should be one input
            assert (
                len(self.get_non_recursive_inputs()) == 1
            ), "Wrong number of translations"
            return {
                self.get_single_non_recursive_input().name: declared_schema_translation
            }
        if isinstance(v, dict):
            return declared_schema_translation
        raise TypeError(declared_schema_translation)

    def get_default_output(self) -> Optional[DataFunctionOutput]:
        return self.outputs.get(DEFAULT_OUTPUT_NAME)


class NonStringAnnotationException(Exception):
    def __init__(self):
        super().__init__(
            "Parameter type annotation must be a string. Try adding "
            "`from __future__ import annotations` to file where function is declared."
        )


def function_interface_from_callable(
    function: DataFunctionCallable,
) -> DataFunctionInterface:
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
            p = parameter_from_annotation(parsed, name=name, default=param.default)
            params[p.name] = p
        else:
            i = function_input_from_parameter(param)
            i = function_input_from_annotation(parsed, name=param.name,)
            inputs[i.name] = i
    return DataFunctionInterface(
        inputs=inputs, outputs=outputs, parameters=params, uses_context=uses_context
    )


def function_input_from_parameter(param: inspect.Parameter) -> DataFunctionInput:
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
    return function_input_from_annotation(parsed, name=param.name,)


def function_input_from_annotation(
    parsed: ParsedAnnotation, name: str
) -> DataFunctionInput:
    return DataFunctionInput(
        name=name,
        schema_like=parsed.schema or "Any",
        input_type=parsed.input_type or InputType.DataBlock,
        required=(not parsed.optional)
        and (parsed.input_type != InputType.SelfReference),
    )


def function_output_from_annotation(
    annotation: Union[str, ParsedAnnotation]
) -> Optional[DataFunctionOutput]:
    if isinstance(annotation, str):
        parsed = parse_output_annotation(annotation)
    else:
        parsed = annotation
    if parsed.is_none:
        return None
    return DataFunctionOutput(
        schema_like=parsed.schema or "Any",
        data_format=parsed.output_type,
        is_iterator=parsed.iterator,
    )


def parameter_from_annotation(
    parsed: ParsedAnnotation, name: str, default: Any
) -> Parameter:
    return Parameter(
        name=name,
        datatype=(
            parsed.parameter_type or ParameterType.Text
        ).value,  # TODO: standardize param datatype to enum
        required=(not parsed.optional),
        default=default,
    )
