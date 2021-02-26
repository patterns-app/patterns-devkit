from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass, field
from typing import Any, TYPE_CHECKING, Dict, List, Optional, Union

from loguru import logger
from snapflow.schema.base import (
    Schema,
    SchemaKey,
    SchemaLike,
    SchemaTranslation,
    is_any,
    is_generic,
)
from sqlalchemy.orm.session import Session

if TYPE_CHECKING:
    from snapflow.core.pipe import (
        InputExhaustedException,
        PipeCallable,
    )
    from snapflow.core.node import Node, Node, NodeLike
    from snapflow.storage.storage import Storage
    from snapflow.core.execution import RunContext
    from snapflow.core.streams import (
        StreamBuilder,
        InputStreams,
        DataBlockStream,
        StreamLike,
    )


re_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)

VALID_DATA_INTERFACE_TYPES = [
    # TODO: is this list just a list of formats? which ones are valid i/o to Pipes?
    # TODO: do we even want this check?
    "Stream",
    "DataBlockStream",
    "Block",
    "DataBlock",
    "DataFrame",
    "Records",
    "RecordsIterator",
    "DataFrameIterator",
    "DatabaseTableRef",
    "DataRecordsObject",
    "Any",
    # "DatabaseCursor",
]

SELF_REF_PARAM_NAME = "this"


class BadAnnotationException(Exception):
    pass


class GenericSchemaException(Exception):
    pass


DEFAULT_INPUT_ANNOTATION = "DataBlock"


@dataclass(frozen=True)
class DeclaredInput:
    name: str
    schema_like: SchemaLike
    data_format: Optional[Any] = None
    reference: bool = False
    required: bool = True
    from_self: bool = False  # TODO: name
    stream: bool = False
    context: bool = False

    # @property
    # def is_context(self) -> bool:
    #     return self.context or self.name == "ctx" or self.data_format == "PipeContext"


@dataclass(frozen=True)
class DeclaredOutput:
    schema_like: SchemaLike
    name: Optional[str] = None
    data_format: Optional[Any] = None
    # reference: bool = False # TODO: not a thing right? that's up to downstream to decide
    optional: bool = False
    default: bool = True
    stream: bool = False


@dataclass(frozen=True)
class DeclaredPipeInterface:
    inputs: List[DeclaredInput]
    output: Optional[DeclaredOutput]

    def get_input(self, name: str) -> DeclaredInput:
        for input in self.inputs:
            if input.name == name:
                return input
        raise KeyError(name)

    def get_non_recursive_inputs(self) -> List[DeclaredInput]:
        return [i for i in self.inputs if not i.from_self]

    def get_inputs_dict(self) -> Dict[str, DeclaredInput]:
        return {i.name: i for i in self.inputs if i.name}

    # TODO: move these assign methods somewhere else?
    def assign_inputs(
        self, inputs: Union[StreamLike, Dict[str, StreamLike]]
    ) -> Dict[str, StreamLike]:
        if not isinstance(inputs, dict):
            assert (
                len(self.get_non_recursive_inputs()) == 1
            ), f"Wrong number of inputs. (Variadic inputs not supported yet) {inputs} {self.get_non_recursive_inputs()}"
            return {self.get_non_recursive_inputs()[0].name: inputs}
        input_names_have = set(inputs.keys())
        input_names_must_have = set(
            i.name for i in self.get_non_recursive_inputs() if i.required
        )
        input_names_ok_to_have = set(i.name for i in self.inputs)
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
                self.get_non_recursive_inputs()[0].name: declared_schema_translation
            }
        if isinstance(v, dict):
            return declared_schema_translation
        raise TypeError(declared_schema_translation)


def pipe_interface_from_callable(pipe: PipeCallable) -> DeclaredPipeInterface:
    requires_context = False
    signature = inspect.signature(pipe)
    output = None
    ret = signature.return_annotation
    if ret is not inspect.Signature.empty:
        if not isinstance(ret, str):
            raise Exception("Return type annotation not a string")
        output = pipe_output_from_annotation(ret)
    inputs = []
    parameters = list(signature.parameters.items())
    for name, param in parameters:
        a = param.annotation
        annotation_is_empty = a is inspect.Signature.empty
        if not annotation_is_empty and not isinstance(a, str):
            raise Exception(
                "Parameter type annotation must be a string. (Try adding `from __future__ import annotations` to file where pipe is declared)"
            )
        i = pipe_input_from_parameter(param)
        inputs.append(i)
    return DeclaredPipeInterface(inputs=inputs, output=output)


def pipe_input_from_parameter(param: inspect.Parameter) -> DeclaredInput:
    a = param.annotation
    annotation_is_empty = a is inspect.Signature.empty
    if not annotation_is_empty and not isinstance(a, str):
        raise Exception(
            "Parameter type annotation not a string. (Try adding `from __future__ import annotations` to your file)"
        )
    annotation = param.annotation
    if annotation is inspect.Signature.empty:
        if param.name in ("ctx", "context"):  # TODO: hack
            annotation = "PipeContext"
        else:
            annotation = DEFAULT_INPUT_ANNOTATION
    is_optional = param.default != inspect.Parameter.empty
    parsed = parse_annotation(annotation)
    return DeclaredInput(
        name=param.name,
        schema_like=parsed.schema_like or "Any",
        data_format=parsed.data_format_class,
        # TODO: can also tell reference from order (all inputs beyond first are def reference, first assumed consumable)
        reference=param.name == SELF_REF_PARAM_NAME,
        required=(not (is_optional or parsed.is_optional)),
        from_self=param.name == SELF_REF_PARAM_NAME,
        stream=parsed.is_stream,
        context=parsed.is_context,
    )


def pipe_output_from_annotation(annotation: str) -> DeclaredOutput:
    parsed = parse_annotation(annotation)
    return DeclaredOutput(
        schema_like=parsed.schema_like or "Any",
        data_format=parsed.data_format_class,
        optional=parsed.is_optional,
    )


def parse_annotation(annotation: str, **kwargs) -> ParsedAnnotation:
    """
    Annotation of form `DataBlock[T]` for example
    """
    if not annotation:
        raise BadAnnotationException(f"Invalid Pipe annotation '{annotation}'")
    m = re_type_hint.match(
        annotation
    )  # TODO: get more strict with matches, for sql comment annotations (would be nice to have example where it fails...?)
    if m is None:
        raise BadAnnotationException(f"Invalid Pipe annotation '{annotation}'")
    is_optional = bool(m.groupdict()["optional"])
    data_format_class = m.groupdict()["origin"]
    schema_name = m.groupdict()["arg"]
    args = dict(
        data_format_class=data_format_class,
        schema_like=schema_name,
        is_optional=is_optional,
        original_annotation=annotation,
    )
    args.update(**kwargs)
    return ParsedAnnotation(**args)


@dataclass(frozen=True)
class ParsedAnnotation:
    data_format_class: str
    schema_like: SchemaLike
    is_optional: bool = False
    original_annotation: Optional[str] = None

    @property
    def is_generic(self) -> bool:
        return is_generic(self.schema_like)

    @property
    def is_stream(self) -> bool:
        return self.data_format_class in {"Stream", "DataBlockStream"}

    @property
    def is_context(self) -> bool:
        return self.data_format_class == "PipeContext"


def make_default_output() -> DeclaredOutput:
    return DeclaredOutput(data_format="Any", schema_like="Any")


def merge_declared_interface_with_signature_interface(
    declared: DeclaredPipeInterface, signature: DeclaredPipeInterface
) -> DeclaredPipeInterface:
    # Merge found and declared
    all_inputs = set(
        [i.name for i in declared.inputs] + [i.name for i in signature.inputs]
    )
    inputs = []
    for name in all_inputs:
        # Declared take precedence
        for i in declared.inputs:
            if i.name == name:
                inputs.append(i)
                break
        else:
            for i in signature.inputs:
                if i.name == name:
                    inputs.append(i)
    output = declared.output or signature.output or make_default_output()
    print(declared.output)
    print(signature.output)
    return DeclaredPipeInterface(inputs=inputs, output=output)

