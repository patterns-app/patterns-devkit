from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from loguru import logger
from snapflow.core.data_block import DataBlock, DataBlockMetadata, ManagedDataBlock
from snapflow.core.environment import Environment
from snapflow.core.typing.schema import (
    Schema,
    SchemaKey,
    SchemaLike,
    SchemaTranslation,
    is_any,
    is_generic,
)

if TYPE_CHECKING:
    from snapflow.core.pipe import (
        InputExhaustedException,
        PipeCallable,
    )
    from snapflow.core.node import Node, Node, NodeLike
    from snapflow.core.storage.storage import Storage
    from snapflow.core.runnable import ExecutionContext
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
    "RecordsList",
    "RecordsListGenerator",
    "DataFrameGenerator",
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
class PipeAnnotation:
    data_format_class: str
    schema_like: SchemaLike
    name: Optional[str] = None
    is_variadic: bool = False  # TODO: what is state of variadic support?
    is_generic: bool = False
    is_optional: bool = False
    is_self_ref: bool = False
    is_stream: bool = False
    original_annotation: Optional[str] = None

    @classmethod
    def create(cls, **kwargs) -> PipeAnnotation:
        if not kwargs.get("schema_like"):
            kwargs["schema_like"] = "Any"
        name = kwargs.get("name")
        if name:
            kwargs["is_self_ref"] = name == SELF_REF_PARAM_NAME
            if kwargs["is_self_ref"]:
                # self-ref is always optional
                kwargs["is_optional"] = True
        schema_name = kwargs.get("schema_like")
        if isinstance(schema_name, str):
            kwargs["is_generic"] = is_generic(schema_name)
        if kwargs["data_format_class"] not in VALID_DATA_INTERFACE_TYPES:
            raise TypeError(
                f"`{kwargs['data_format_class']}` is not a valid data input type"
            )
        if kwargs["data_format_class"] in {"Stream", "DataBlockStream"}:
            kwargs["is_stream"] = True
        return PipeAnnotation(**kwargs)

    @classmethod
    def from_parameter(cls, parameter: inspect.Parameter) -> PipeAnnotation:
        annotation = parameter.annotation
        if annotation is inspect.Signature.empty:
            if parameter.name not in ("ctx", "context"):  # TODO: hack
                annotation = DEFAULT_INPUT_ANNOTATION
        is_optional = parameter.default != inspect.Parameter.empty
        is_variadic = parameter.kind == inspect.Parameter.VAR_POSITIONAL
        tda = cls.from_type_annotation(
            annotation,
            name=parameter.name,
            is_optional=is_optional,
            is_variadic=is_variadic,
        )
        return tda

    @classmethod
    def from_type_annotation(cls, annotation: str, **kwargs) -> PipeAnnotation:
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
        return PipeAnnotation.create(**args)  # type: ignore

    def schema(self, env: Environment) -> Schema:
        if self.is_generic:
            raise GenericSchemaException("Generic Schema has no name")
        return env.get_schema(self.schema_like)

    def schema_key(self, env: Environment) -> SchemaKey:
        return self.schema(env).key


def make_default_output_annotation():
    return PipeAnnotation.create(
        data_format_class="Any",
        schema_like="Any",
    )


@dataclass(frozen=True)
class PipeInterface:
    inputs: List[PipeAnnotation]
    output: Optional[PipeAnnotation]
    requires_pipe_context: bool = True

    @classmethod
    def create(cls, **kwargs) -> PipeInterface:
        output = kwargs.get("output")
        if output is None:
            kwargs["output"] = make_default_output_annotation()
        pi = cls(**kwargs)  # type: ignore
        pi.validate_inputs()  # TODO: let caller handle this if they want?
        return pi

    @classmethod
    def from_pipe_definition(cls, df: PipeCallable) -> PipeInterface:
        requires_context = False
        signature = inspect.signature(df)
        output = None
        ret = signature.return_annotation
        if ret is not inspect.Signature.empty:
            if not isinstance(ret, str):
                raise Exception("Return type annotation not a string")
            output = PipeAnnotation.from_type_annotation(ret)
        inputs = []
        parameters = list(signature.parameters.items())
        for name, param in parameters:
            a = param.annotation
            annotation_is_empty = a is inspect.Signature.empty
            if not annotation_is_empty and not isinstance(a, str):
                raise Exception(
                    "Parameter type annotation not a string. (Try adding `from __future__ import annotations` to your file)"
                )
            try:
                a = PipeAnnotation.from_parameter(param)
                inputs.append(a)
            except TypeError:
                # Not a DataBlock/Set
                if (
                    param.annotation == "PipeContext" or name == "ctx"
                ):  # TODO: hidden constants
                    requires_context = True
                else:
                    raise Exception(f"Invalid data pipe parameter {param}")
        return cls.create(
            inputs=inputs,
            output=output,
            requires_pipe_context=requires_context,
        )

    def get_input(self, name: str) -> PipeAnnotation:
        for input in self.inputs:
            if input.name == name:
                return input
        raise KeyError(name)

    def get_non_recursive_inputs(self) -> List[PipeAnnotation]:
        return [i for i in self.inputs if not i.is_self_ref]

    def get_inputs_dict(self) -> Dict[str, PipeAnnotation]:
        return {i.name: i for i in self.inputs if i.name}

    def validate_inputs(self):
        pass
        # TODO: Up to user to ensure their graph makes sense for pipe?
        # data_block_seen = False
        # for annotation in self.inputs:
        #     if (
        #         annotation.data_format_class == "DataBlock"
        #         and not annotation.is_optional
        #     ):
        #         if data_block_seen:
        #             raise Exception(
        #                 "Only one uncorrelated DataBlock input allowed to a Pipe."
        #                 "Correlate the inputs or use a DataSet"
        #             )
        #         data_block_seen = True

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
            i.name for i in self.get_non_recursive_inputs() if not i.is_optional
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

    def connect(
        self, declared_inputs: Dict[str, DeclaredStreamInput]
    ) -> ConnectedInterface:
        inputs = []
        for annotation in self.inputs:
            input_stream_builder = None
            translation = None
            assert annotation.name is not None
            dni = declared_inputs.get(annotation.name)
            if dni:
                input_stream_builder = dni.stream
                translation = dni.declared_schema_translation
            ni = NodeInput(
                name=annotation.name,
                annotation=annotation,
                input_stream_builder=input_stream_builder,
                declared_schema_translation=translation,
            )
            inputs.append(ni)
        return ConnectedInterface(
            inputs=inputs,
            output=self.output,
            requires_pipe_context=self.requires_pipe_context,
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
    annotation: PipeAnnotation
    declared_schema_translation: Optional[Dict[str, str]] = None
    input_stream_builder: Optional[StreamBuilder] = None


@dataclass(frozen=True)
class ConnectedInterface:
    inputs: List[NodeInput]
    output: Optional[PipeAnnotation]
    requires_pipe_context: bool = True

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
                if not node_input.annotation.is_stream:
                    # TODO: handle StopIteration here? Happens if `get_bound_interface` is passed empty stream
                    #   (will trigger an InputExhastedException earlier otherwise)
                    bound_block = next(dbs)
            si = StreamInput(
                name=node_input.name,
                annotation=node_input.annotation,
                declared_schema_translation=node_input.declared_schema_translation,
                input_stream_builder=node_input.input_stream_builder,
                is_stream=node_input.annotation.is_stream,
                bound_stream=bound_stream,
                bound_block=bound_block,
            )
            inputs.append(si)
        return BoundInterface(
            inputs=inputs,
            output=self.output,
            requires_pipe_context=self.requires_pipe_context,
        )


@dataclass(frozen=True)
class StreamInput:
    name: str
    annotation: PipeAnnotation
    declared_schema_translation: Optional[Dict[str, str]] = None
    input_stream_builder: Optional[StreamBuilder] = None
    is_stream: bool = False
    bound_stream: Optional[DataBlockStream] = None
    bound_block: Optional[DataBlock] = None

    def get_bound_nominal_schema(self) -> Optional[Schema]:
        # TODO: what is this and what is this called? "resolved"?
        if self.bound_block:
            return self.bound_block.nominal_schema
        if self.bound_stream:
            emitted = self.bound_stream.get_emitted_managed_blocks()
            if not emitted:
                # if self.bound_stream.count():
                #     logger.warning("No blocks emitted yet from non-empty stream")
                return None
            return emitted[0].nominal_schema
        return None


@dataclass(frozen=True)
class BoundInterface:
    inputs: List[StreamInput]
    output: Optional[PipeAnnotation]
    requires_pipe_context: bool = True
    # resolved_generics: Dict[str, SchemaKey] = field(default_factory=dict)

    def inputs_as_kwargs(self) -> Dict[str, Union[DataBlock, DataBlockStream]]:
        return {
            i.name: i.bound_stream if i.is_stream else i.bound_block
            for i in self.inputs
            if i.bound_stream is not None
        }

    def resolve_nominal_output_schema(self, env: Environment) -> Optional[Schema]:
        if not self.output:
            return None
        if not self.output.is_generic:
            return self.output.schema(env)
        output_generic = self.output.schema_like
        for input in self.inputs:
            if not input.annotation.is_generic:
                continue
            if input.annotation.schema_like == output_generic:
                schema = input.get_bound_nominal_schema()
                # We check if None -- there may be more than one input with same generic, we'll take any that are resolvable
                if schema is not None:
                    return schema
        raise Exception(f"Unable to resolve generic '{output_generic}'")


def get_schema_translation(
    env: Environment,
    data_block: DataBlockMetadata,
    declared_schema: Optional[Schema] = None,
    declared_schema_translation: Optional[Dict[str, str]] = None,
) -> Optional[SchemaTranslation]:
    if declared_schema_translation:
        # If we are given a declared translation, then that overrides a natural translation
        return SchemaTranslation(
            translation=declared_schema_translation,
            from_schema=data_block.realized_schema(env),
        )
    if declared_schema is None or is_any(declared_schema):
        # Nothing expected, so no translation needed
        return None
    # Otherwise map found schema to expected schema
    return data_block.realized_schema(env).get_translation_to(env, declared_schema)


#
# @dataclass
# class BoundPipeInterface:
#     inputs: List[NodeInput]
#     output: Optional[PipeAnnotation]
#     requires_pipe_context: bool = True
#     resolved_generics: Dict[str, SchemaKey] = field(default_factory=dict)
#     manually_set_resolved_output_schema: Optional[
#         Schema
#     ] = None  # TODO: move to PipeContext?
#
#     def get_input(self, name: str) -> NodeInput:
#         for input in self.inputs:
#             if input.name == name:
#                 return input
#         raise KeyError(name)
#
#     def connect(self, input_nodes: Dict[str, Node]):
#         for name, input_node in input_nodes.items():
#             i = self.get_input(name)
#             i.input_node = input_node
#
#     def bind(self, input_block_streams: Dict[str, DataBlockStreamBuilder]):
#         for name, dbs in input_block_streams.items():
#             i = self.get_input(name)
#             i.bound_data_block_stream = dbs
#             if i.annotation.is_generic:
#                 self.resolved_generics[
#                     i.annotation.schema_like
#                 ] = i.bound_data_block.most_abstract_schema_key
#
#     @classmethod
#     def from_dfi(cls, pi: PipeInterface) -> BoundPipeInterface:
#         return BoundPipeInterface(
#             inputs=[NodeInput(name=a.name, original_annotation=a) for a in pi.inputs],
#             output=pi.output,
#             requires_pipe_context=pi.requires_pipe_context,
#         )
#
#     # def inputs_as_kwargs(self):
#     #     return {
#     #         i.name: i.bound_data_block
#     #         for i in self.inputs
#     #         if i.bound_data_block is not None
#     #     }
#     #
#     # def inputs_as_managed_data_blocks(
#     #     self, ctx: ExecutionContext
#     # ) -> Dict[str, ManagedDataBlock]:
#     #     inputs: Dict[str, ManagedDataBlock] = {}
#     #     for i in self.inputs:
#     #         if i.bound_data_block is None:
#     #             continue
#     #         inputs[i.name] = i.bound_data_block.as_managed_data_block(
#     #             ctx, translation=i.get_schema_translation(ctx.env)
#     #         )
#     #     return inputs
#
#     def resolved_output_schema(self, env: Environment) -> Optional[Schema]:
#         if self.manually_set_resolved_output_schema is not None:
#             return self.manually_set_resolved_output_schema
#         if self.output is None:
#             return None
#         if self.output.is_generic:
#             k = self.resolved_generics[self.output.schema_like]
#             return env.get_schema(k)
#         return self.output.schema(env)
#
#     def set_resolved_output_schema(self, schema: Schema):
#         self.manually_set_resolved_output_schema = schema
#

#
#     def bind_and_specify_schemas(self, env: Environment, input_blocks: InputBlocks):
#         if self.is_bound:
#             raise Exception("Already bound")
#         realized_generics: Dict[str, Schema] = {}
#         for name, input_block in input_blocks.items():
#             i = self.get_input(name)
#             i.bound_data_block = input_block
#             i.realized_schema = env.get_schema(input_block.realized_schema_key)
#             if i.original_annotation.is_generic:
#                 assert isinstance(i.original_annotation.schema_like, str)
#                 realized_generics[i.original_annotation.schema_like] = i.realized_schema
#         if (
#             self.output is not None
#             and is_any(self.resolved_output_schema)
#             and self.output.is_generic
#         ):
#             # Further specify resolved type now that we have something concrete for Any
#             # TODO: man this is too complex. how do we simplify different type levels
#             assert isinstance(self.output.schema_like, str)
#             self.resolved_output_schema = realized_generics[self.output.schema_like]
#         self.is_bound = True
#
#     def as_kwargs(self):
#         if not self.is_bound:
#             raise Exception("Interface not bound")
#         return {i.name: i.bound_data_block for i in self.inputs}


class NodeInterfaceManager:
    """
    Responsible for finding and preparing DataBlocks for input to a
    Node.
    """

    def __init__(self, ctx: ExecutionContext, node: Node, strict_storages: bool = True):
        self.env = ctx.env
        self.ctx = ctx
        self.node = node
        self.pipe_interface: PipeInterface = self.node.get_interface()
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
        for annotation in self.pipe_interface.inputs:
            if annotation.is_self_ref:
                inputs["this"] = DeclaredStreamInput(
                    stream=self.node.as_stream_builder(),
                    declared_schema_translation=self.node.get_schema_translation_for_input(
                        "this"
                    ),
                )
        ci = self.pipe_interface.connect(inputs)
        return ci

    def is_input_required(self, annotation: PipeAnnotation) -> bool:
        if annotation.is_optional:
            return False
        return True

    def get_input_data_block_streams(self) -> InputStreams:
        from snapflow.core.pipe import InputExhaustedException

        logger.debug(f"GETTING INPUTS for {self.node.key}")
        input_streams: InputStreams = {}
        any_unprocessed = False
        for input in self.get_connected_interface().inputs:
            stream_builder = input.input_stream_builder
            if stream_builder is None:
                if input.annotation.is_optional:
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
            if stream_builder.get_count(self.ctx) == 0:
                logger.debug(
                    f"Couldnt find eligible DataBlocks for input `{input.name}` from {stream_builder}"
                )
                if not input.annotation.is_optional:
                    raise InputExhaustedException(
                        f"    Required input '{input.name}'={stream_builder} to Pipe '{self.node.key}' is empty"
                    )
            else:
                declared_schema: Optional[Schema]
                try:
                    declared_schema = input.annotation.schema(self.env)
                except GenericSchemaException:
                    declared_schema = None
                input_streams[input.name] = stream_builder.as_managed_stream(
                    self.ctx,
                    declared_schema=declared_schema,
                    declared_schema_translation=input.declared_schema_translation,
                )
            any_unprocessed = True

        if input_streams and not any_unprocessed:
            # TODO: is this really an exception always?
            raise InputExhaustedException("All inputs exhausted")

        return input_streams

    def _filter_stream(
        self,
        stream_builder: StreamBuilder,
        input: NodeInput,
        storages: List[Storage] = None,
    ) -> StreamBuilder:
        logger.debug(f"{stream_builder.get_count(self.ctx)} available DataBlocks")
        if storages:
            stream_builder = stream_builder.filter_storages(storages)
            logger.debug(
                f"{stream_builder.get_count(self.ctx)} available DataBlocks in storages {storages}"
            )
        logger.debug(f"Finding unprocessed input for: {stream_builder}")
        stream_builder = stream_builder.filter_unprocessed(
            self.node, allow_cycle=input.annotation.is_self_ref
        )
        logger.debug(f"{stream_builder.get_count(self.ctx)} unprocessed DataBlocks")
        return stream_builder
