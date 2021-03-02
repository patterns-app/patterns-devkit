from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from loguru import logger
from snapflow.core.data_block import DataBlock
from snapflow.core.environment import Environment
from snapflow.schema.base import (
    GenericSchemaException,
    Schema,
    SchemaKey,
    SchemaLike,
    SchemaTranslation,
    is_any,
    is_generic,
)
from sqlalchemy.orm.session import Session

if TYPE_CHECKING:
    from snapflow.core.snap import (
        InputExhaustedException,
        SnapCallable,
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
    # TODO: is this list just a list of formats? which ones are valid i/o to Snaps?
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


DEFAULT_INPUT_ANNOTATION = "DataBlock"


@dataclass(frozen=True)
class DeclaredInput:
    name: str
    schema_like: SchemaLike
    data_format: str = "DataBlock"
    reference: bool = False
    _required: bool = True
    from_self: bool = False  # TODO: name
    stream: bool = False
    context: bool = False

    @property
    def required(self) -> bool:
        # Can't require recursive input!
        return (not self.from_self) and self._required

    @property
    def is_generic(self) -> bool:
        return is_generic(self.schema_like)


@dataclass(frozen=True)
class DeclaredOutput:
    schema_like: SchemaLike
    name: Optional[str] = None
    data_format: Optional[Any] = None
    # reference: bool = False # TODO: not a thing right? that's up to downstream to decide
    optional: bool = False
    default: bool = True
    stream: bool = False

    @property
    def is_generic(self) -> bool:
        return is_generic(self.schema_like)


DEFAULT_CONTEXT = DeclaredInput(
    name="ctx",
    schema_like="Any",
    data_format="SnapContext",
    reference=False,
    _required=True,
    from_self=False,
    stream=False,
    context=True,
)


@dataclass(frozen=True)
class DeclaredSnapInterface:
    inputs: List[DeclaredInput]
    output: Optional[DeclaredOutput] = None
    context: Optional[DeclaredInput] = None

    def get_input(self, name: str) -> DeclaredInput:
        for input in self.inputs:
            if input.name == name:
                return input
        raise KeyError(name)

    def get_non_recursive_inputs(self) -> List[DeclaredInput]:
        return [i for i in self.inputs if not i.from_self and not i.context]

    def get_inputs_dict(self) -> Dict[str, DeclaredInput]:
        return {i.name: i for i in self.inputs if i.name and not i.context}

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

    # TODO: move
    def connect(
        self, declared_inputs: Dict[str, DeclaredStreamInput]
    ) -> ConnectedInterface:
        inputs = []
        for input in self.inputs:
            input_stream_builder = None
            translation = None
            assert input.name is not None
            dni = declared_inputs.get(input.name)
            if dni:
                input_stream_builder = dni.stream
                translation = dni.declared_schema_translation
            ni = NodeInput(
                name=input.name,
                declared_input=input,
                input_stream_builder=input_stream_builder,
                declared_schema_translation=translation,
            )
            inputs.append(ni)
        return ConnectedInterface(
            inputs=inputs, output=self.output, context=self.context
        )


def snap_interface_from_callable(snap: SnapCallable) -> DeclaredSnapInterface:
    signature = inspect.signature(snap)
    output = None
    context = None
    ret = signature.return_annotation
    if ret is not inspect.Signature.empty:
        if not isinstance(ret, str):
            raise Exception("Return type annotation not a string")
        output = snap_output_from_annotation(ret)
    inputs = []
    parameters = list(signature.parameters.items())
    for name, param in parameters:
        a = param.annotation
        annotation_is_empty = a is inspect.Signature.empty
        if not annotation_is_empty and not isinstance(a, str):
            raise Exception(
                "Parameter type annotation must be a string. (Try adding `from __future__ import annotations` to file where snap is declared)"
            )
        i = snap_input_from_parameter(param)
        if i.context:
            assert context is None
            context = i
        else:
            inputs.append(i)
    return DeclaredSnapInterface(inputs=inputs, output=output, context=context)


def snap_input_from_parameter(param: inspect.Parameter) -> DeclaredInput:
    a = param.annotation
    annotation_is_empty = a is inspect.Signature.empty
    if not annotation_is_empty and not isinstance(a, str):
        raise Exception(
            "Parameter type annotation not a string. (Try adding `from __future__ import annotations` to your file)"
        )
    annotation = param.annotation
    if annotation is inspect.Signature.empty:
        if param.name in ("ctx", "context"):  # TODO: hack
            annotation = "SnapContext"
        else:
            annotation = DEFAULT_INPUT_ANNOTATION
    is_optional = param.default != inspect.Parameter.empty
    parsed = parse_annotation(annotation)
    return snap_input_from_annotation(parsed, name=param.name, is_optional=is_optional)


def snap_input_from_annotation(
    parsed: ParsedAnnotation, name: str, is_optional: bool = False
) -> DeclaredInput:
    return DeclaredInput(
        name=name,
        schema_like=parsed.schema_like or "Any",
        data_format=parsed.data_format_class,
        # TODO: can also tell reference from order (all inputs beyond first are def reference, first assumed consumable)
        reference=name == SELF_REF_PARAM_NAME,
        _required=(not (is_optional or parsed.is_optional)),
        from_self=name == SELF_REF_PARAM_NAME,
        stream=parsed.is_stream,
        context=parsed.is_context,
    )


def snap_output_from_annotation(
    annotation: Union[str, ParsedAnnotation]
) -> DeclaredOutput:
    if isinstance(annotation, str):
        parsed = parse_annotation(annotation)
    else:
        parsed = annotation
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
        raise BadAnnotationException(f"Invalid _Snap annotation '{annotation}'")
    m = re_type_hint.match(
        annotation
    )  # TODO: get more strict with matches, for sql comment annotations (would be nice to have example where it fails...?)
    if m is None:
        raise BadAnnotationException(f"Invalid _Snap annotation '{annotation}'")
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
        return self.data_format_class == "SnapContext"


def make_default_output() -> DeclaredOutput:
    return DeclaredOutput(data_format="Any", schema_like="Any")


def merge_declared_interface_with_signature_interface(
    declared: DeclaredSnapInterface,
    signature: DeclaredSnapInterface,
    ignore_signature: bool = False,
) -> DeclaredSnapInterface:
    # ctx can come from EITHER
    # Take union of inputs from both, with declared taking precedence
    # UNLESS ignore_signature, then only use signature if NO declared inputs
    if ignore_signature and declared.inputs:
        inputs = declared.inputs
    else:
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
    return DeclaredSnapInterface(
        inputs=inputs, output=output, context=declared.context or signature.context
    )


@dataclass(frozen=True)
class DeclaredStreamLikeInput:
    stream_like: StreamLike
    declared_schema_translation: Optional[Dict[str, str]] = None


@dataclass(frozen=True)
class DeclaredStreamInput:
    stream: StreamBuilder
    declared_schema_translation: Optional[Dict[str, str]] = None


@dataclass(frozen=True)
class NodeInput:
    name: str
    declared_input: DeclaredInput
    declared_schema_translation: Optional[Dict[str, str]] = None
    input_stream_builder: Optional[StreamBuilder] = None


@dataclass(frozen=True)
class ConnectedInterface:
    inputs: List[NodeInput]
    output: Optional[DeclaredOutput] = None
    context: Optional[DeclaredInput] = None

    def get_input(self, name: str) -> NodeInput:
        for input in self.inputs:
            if input.name == name:
                return input
        raise KeyError(name)

    def bind(self, input_streams: InputStreams) -> BoundInterface:
        inputs = []
        for node_input in self.inputs:
            dbs = input_streams.get(node_input.name)
            bound_stream = None
            bound_block = None
            if dbs is not None:
                bound_stream = dbs
                if not node_input.declared_input.stream:
                    # TODO: handle StopIteration here? Happens if `get_bound_interface` is passed empty stream
                    #   (will trigger an InputExhastedException earlier otherwise)
                    bound_block = next(dbs)
            si = StreamInput(
                name=node_input.name,
                declared_input=node_input.declared_input,
                declared_schema_translation=node_input.declared_schema_translation,
                input_stream_builder=node_input.input_stream_builder,
                is_stream=node_input.declared_input.stream,
                bound_stream=bound_stream,
                bound_block=bound_block,
            )
            inputs.append(si)
        return BoundInterface(inputs=inputs, output=self.output, context=self.context)


@dataclass(frozen=True)
class StreamInput:
    name: str
    declared_input: DeclaredInput
    declared_schema_translation: Optional[Dict[str, str]] = None
    input_stream_builder: Optional[StreamBuilder] = None
    is_stream: bool = False
    bound_stream: Optional[DataBlockStream] = None
    bound_block: Optional[DataBlock] = None

    def get_bound_block_property(self, prop: str):
        if self.bound_block:
            return getattr(self.bound_block, prop)
        if self.bound_stream:
            emitted = self.bound_stream.get_emitted_managed_blocks()
            if not emitted:
                # if self.bound_stream.count():
                #     logger.warning("No blocks emitted yet from non-empty stream")
                return None
            return getattr(emitted[0], prop)
        return None

    def get_bound_nominal_schema(self) -> Optional[Schema]:
        # TODO: what is this and what is this called? "resolved"?
        return self.get_bound_block_property("nominal_schema")

    @property
    def nominal_schema(self) -> Optional[Schema]:
        return self.get_bound_block_property("nominal_schema")

    @property
    def realized_schema(self) -> Optional[Schema]:
        return self.get_bound_block_property("realized_schema")


@dataclass(frozen=True)
class BoundInterface:
    inputs: List[StreamInput]
    output: Optional[DeclaredOutput] = None
    context: Optional[DeclaredInput] = None
    # resolved_generics: Dict[str, SchemaKey] = field(default_factory=dict)

    def inputs_as_kwargs(self) -> Dict[str, Union[DataBlock, DataBlockStream]]:
        return {
            i.name: i.bound_stream if i.is_stream else i.bound_block
            for i in self.inputs
            if i.bound_stream is not None
        }

    def resolve_nominal_output_schema(
        self, env: Environment, sess: Session
    ) -> Optional[Schema]:
        if not self.output:
            return None
        if not self.output.is_generic:
            return env.get_schema(self.output.schema_like, sess)
        output_generic = self.output.schema_like
        for input in self.inputs:
            if not input.declared_input.is_generic:
                continue
            if input.declared_input.schema_like == output_generic:
                schema = input.get_bound_nominal_schema()
                # We check if None -- there may be more than one input with same generic, we'll take any that are resolvable
                if schema is not None:
                    return schema
        raise Exception(f"Unable to resolve generic '{output_generic}'")


def get_schema_translation(
    env: Environment,
    sess: Session,
    source_schema: Schema,
    target_schema: Optional[Schema] = None,
    declared_schema_translation: Optional[Dict[str, str]] = None,
) -> Optional[SchemaTranslation]:
    # THE place to determine requested/necessary schema translation
    if declared_schema_translation:
        # If we are given a declared translation, then that overrides a natural translation
        return SchemaTranslation(
            translation=declared_schema_translation,
            from_schema=source_schema,
        )
    if target_schema is None or is_any(target_schema):
        # Nothing expected, so no translation needed
        return None
    # Otherwise map found schema to expected schema
    return source_schema.get_translation_to(env, sess, target_schema)


class NodeInterfaceManager:
    """
    Responsible for finding and preparing DataBlocks for input to a
    Node.
    """

    def __init__(
        self, ctx: RunContext, sess: Session, node: Node, strict_storages: bool = True
    ):
        self.env = ctx.env
        self.sess = sess
        self.ctx = ctx
        self.node = node
        self.snap_interface: DeclaredSnapInterface = self.node.get_interface()
        self.strict_storages = (
            strict_storages  # Only pull datablocks from given storages
        )

    def get_bound_interface(
        self, input_db_streams: Optional[InputStreams] = None
    ) -> BoundInterface:
        ci = self.get_connected_interface()
        if input_db_streams is None:
            input_db_streams = self.get_input_data_block_streams()
        return ci.bind(input_db_streams)

    def get_connected_interface(self) -> ConnectedInterface:
        inputs = self.node.declared_inputs
        # Add "this" if it has a self-ref (TODO: a bit hidden down here no?)
        for input in self.snap_interface.inputs:
            if input.from_self:
                inputs[input.name] = DeclaredStreamInput(
                    stream=self.node.as_stream_builder(),
                    declared_schema_translation=self.node.get_schema_translation_for_input(
                        input.name
                    ),
                )
        ci = self.snap_interface.connect(inputs)
        return ci

    def is_input_required(self, input: DeclaredInput) -> bool:
        # TODO: is there other logic we want here? why have method?
        if input.required:
            return True
        return False

    def get_input_data_block_streams(self) -> InputStreams:
        from snapflow.core.snap import InputExhaustedException

        logger.debug(f"GETTING INPUTS for {self.node.key}")
        input_streams: InputStreams = {}
        any_unprocessed = False
        for input in self.get_connected_interface().inputs:
            stream_builder = input.input_stream_builder
            if stream_builder is None:
                if not input.declared_input.required:
                    continue
                raise Exception(f"Missing required input {input.name}")
            logger.debug(f"Building stream for `{input.name}` from {stream_builder}")
            stream_builder = self._filter_stream(
                stream_builder,
                input,
                self.ctx.all_storages if self.strict_storages else None,
            )

            """
            Inputs are considered "Exhausted" if:
            - Single block stream (and zero or more DSs): no unprocessed blocks
            - Multiple correlated block streams: ANY stream has no unprocessed blocks
            - One or more DSs: if ALL DS streams have no unprocessed

            In other words, if ANY block stream is empty, bail out. If ALL DS streams are empty, bail
            """
            if stream_builder.get_count(self.ctx, self.sess) == 0:
                logger.debug(
                    f"Couldnt find eligible DataBlocks for input `{input.name}` from {stream_builder}"
                )
                if input.declared_input.required:
                    raise InputExhaustedException(
                        f"    Required input '{input.name}'={stream_builder} to _Snap '{self.node.key}' is empty"
                    )
            else:
                declared_schema: Optional[Schema]
                try:
                    declared_schema = self.env.get_schema(
                        input.declared_input.schema_like, self.sess
                    )
                except GenericSchemaException:
                    declared_schema = None
                input_streams[input.name] = stream_builder.as_managed_stream(
                    self.ctx,
                    self.sess,
                    declared_schema=declared_schema,
                    declared_schema_translation=input.declared_schema_translation,
                )
            any_unprocessed = True

        if input_streams and not any_unprocessed:
            # TODO: is this really an exception always?
            logger.debug("Inputs exhausted")
            raise InputExhaustedException("All inputs exhausted")

        return input_streams

    def _filter_stream(
        self,
        stream_builder: StreamBuilder,
        input: NodeInput,
        storages: List[Storage] = None,
    ) -> StreamBuilder:
        logger.debug(
            f"{stream_builder.get_count(self.ctx, self.sess)} available DataBlocks"
        )
        if storages:
            stream_builder = stream_builder.filter_storages(storages)
            logger.debug(
                f"{stream_builder.get_count(self.ctx, self.sess)} available DataBlocks in storages {storages}"
            )
        logger.debug(f"Finding unprocessed input for: {stream_builder}")
        stream_builder = stream_builder.filter_unprocessed(
            self.node, allow_cycle=input.declared_input.from_self
        )
        logger.debug(
            f"{stream_builder.get_count(self.ctx, self.sess)} unprocessed DataBlocks"
        )
        return stream_builder
