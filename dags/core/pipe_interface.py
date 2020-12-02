from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import networkx as nx

from dags.core.data_block import DataBlock, DataBlockMetadata, ManagedDataBlock
from dags.core.environment import Environment
from dags.core.typing.object_schema import (
    ObjectSchema,
    ObjectSchemaKey,
    ObjectSchemaLike,
    SchemaMapping,
    is_any,
    is_generic,
)
from dags.utils.common import printd
from dags.utils.typing import T
from loguru import logger

if TYPE_CHECKING:
    from dags.core.pipe import (
        InputExhaustedException,
        PipeCallable,
    )
    from dags.core.node import Node, Node, NodeLike
    from dags.core.storage.storage import Storage
    from dags.core.runnable import ExecutionContext
    from dags.core.streams import (
        InputBlocks,
        DataBlockStreamBuilder,
        ensure_data_stream,
        PipeNodeInput,
        InputStreams,
        DataBlockStream,
    )


# re_type_hint = re.compile(
#     r"(?P<iterable>(Iterator|Iterable|Sequence|List)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
# )
re_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)

VALID_DATA_INTERFACE_TYPES = [
    "Stream",
    "DataBlockStream" "Block",
    "DataBlock",
    "DataFrame",
    "RecordsList",
    "RecordsListGenerator",
    "DataFrameGenerator",
    "DatabaseTableRef",
    "Any",  # TODO: does this work?
    # TODO: is this list just a list of formats? which ones are valid i/o to Pipes?
    # TODO: also, are DataBlocks and DataSets the only valid *input* types? A bit confusing to end user I think
    # "DatabaseCursor",
]

SELF_REF_PARAM_NAME = "this"


class BadAnnotationException(Exception):
    pass


class GenericSchemaException(Exception):
    pass


@dataclass(frozen=True)
class PipeAnnotation:
    data_format_class: str
    schema_like: ObjectSchemaLike
    name: Optional[str] = None
    # is_iterable: bool = False  # TODO: what is state of iterable support?
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
        # is_optional = parameter.default != inspect.Parameter.empty
        is_variadic = parameter.kind == inspect.Parameter.VAR_POSITIONAL
        tda = cls.from_type_annotation(
            annotation,
            name=parameter.name,
            # is_optional=is_optional,
            is_variadic=is_variadic,
        )
        return tda

    @classmethod
    def from_type_annotation(cls, annotation: str, **kwargs) -> PipeAnnotation:
        """
        Annotation of form `DataBlock[T]` for example
        """
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

    def schema(self, env: Environment) -> ObjectSchema:
        if self.is_generic:
            raise GenericSchemaException("Generic ObjectSchema has no name")
        return env.get_schema(self.schema_like)

    def schema_key(self, env: Environment) -> ObjectSchemaKey:
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
        for name, param in signature.parameters.items():
            a = param.annotation
            if a is not inspect.Signature.empty:
                if not isinstance(a, str):
                    raise Exception("Parameter type annotation not a string")
            try:
                a = PipeAnnotation.from_parameter(param)
                inputs.append(a)
            except TypeError:
                # Not a DataBlock/Set
                if param.annotation == "PipeContext":
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

    def get_non_recursive_inputs(self):
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

    def assign_inputs(self, inputs: Union[T, Dict[str, T]]) -> Dict[str, T]:
        if not isinstance(inputs, dict):
            assert (
                len(self.get_non_recursive_inputs()) == 1
            ), f"Wrong number of inputs. (Variadic inputs not supported yet) {inputs} {self.get_non_recursive_inputs()}"
            return {self.get_non_recursive_inputs()[0].name: inputs}
        assert (set(inputs.keys()) - {"this"}) == set(
            i.name for i in self.get_non_recursive_inputs()
        ), f"{inputs}  {self.get_non_recursive_inputs()}"
        return inputs

    def assign_mapping(
        self, declared_schema_mapping: Optional[Dict[str, Union[Dict[str, str], str]]]
    ) -> Optional[Dict[str, Dict[str, str]]]:
        if not declared_schema_mapping:
            return None
        v = list(declared_schema_mapping.values())[0]
        if isinstance(v, str):
            # Just one mapping, so should be one input
            assert (
                len(self.get_non_recursive_inputs()) == 1
            ), f"Wrong number of mappings"
            return {self.get_non_recursive_inputs()[0].name: declared_schema_mapping}
        if isinstance(v, dict):
            return declared_schema_mapping
        raise TypeError(declared_schema_mapping)

    def connect(self, input_nodes: Dict[str, DeclaredNodeInput]) -> ConnectedInterface:
        return ConnectedInterface(
            inputs=[
                NodeInput(
                    name=name,
                    annotation=self.get_input(name),
                    input_node=node_input.node,
                    declared_schema_mapping=node_input.declared_schema_mapping,
                )
                for name, node_input in input_nodes.items()
            ],
            output=self.output,
            requires_pipe_context=self.requires_pipe_context,
        )


@dataclass(frozen=True)
class DeclaredNodeLikeInput:
    node_like: NodeLike
    declared_schema_mapping: Optional[Dict[str, str]] = None


@dataclass(frozen=True)
class DeclaredNodeInput:
    node: Node
    declared_schema_mapping: Optional[Dict[str, str]] = None


@dataclass(frozen=True)
class NodeInput:
    name: str
    annotation: PipeAnnotation
    declared_schema_mapping: Optional[Dict[str, str]] = None
    input_node: Optional[Node] = None

    @property
    def env(self) -> Optional[Environment]:
        if self.input_node is None:
            return None
        return self.input_node.env


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
            d = asdict(node_input)
            d["is_stream"] = node_input.annotation.is_stream
            dbs = input_streams.get(node_input.name)
            if dbs is not None:
                d["bound_stream"] = dbs
                if not node_input.annotation.is_stream:
                    d["bound_block"] = dbs.next()
            inputs.append(StreamInput(**d))
        return BoundInterface(
            inputs=inputs,
            output=self.output,
            requires_pipe_context=self.requires_pipe_context,
        )


@dataclass(frozen=True)
class StreamInput:
    name: str
    annotation: PipeAnnotation
    declared_schema_mapping: Optional[Dict[str, str]] = None
    input_node: Optional[Node] = None
    is_stream: bool = False
    bound_stream: Optional[DataBlockStream] = None
    bound_block: Optional[DataBlock] = None

    def get_resolved_schema(self) -> Optional[ObjectSchema]:
        # TODO: what is this and what is this called? "resolved"?
        if self.bound_block:
            return self.bound_block.most_abstract_schema
        if self.bound_stream:
            emitted = self.bound_stream.get_emitted_managed_blocks()
            if not emitted:
                if self.bound_stream.count():
                    logger.warning(f"No blocks emitted yet from non-empty stream")
                return None
            return emitted[0].most_abstract_schema
        return None


@dataclass(frozen=True)
class BoundInterface:
    inputs: List[StreamInput]
    output: Optional[PipeAnnotation]
    requires_pipe_context: bool = True
    resolved_generics: Dict[str, ObjectSchemaKey] = field(default_factory=dict)

    def inputs_as_kwargs(self) -> Dict[str, Union[DataBlock, DataBlockStream]]:
        return {
            i.name: i.bound_stream if i.is_stream else i.bound_block
            for i in self.inputs
            if i.bound_stream is not None
        }


def resolve_output_generic(
    inputs: List[StreamInput], output_annotation: PipeAnnotation
) -> Optional[ObjectSchema]:
    assert output_annotation.is_generic
    output_generic = output_annotation.schema_like
    for input in inputs:
        if not input.annotation.is_generic:
            continue
        if input.annotation.schema_like == output_generic:
            return input.get_resolved_schema()
    raise Exception(f"Unable to resolve generic '{output_generic}'")


def get_schema_mapping(
    env: Environment,
    data_block: DataBlockMetadata,
    expected_schema: Optional[ObjectSchema] = None,
    declared_schema_mapping: Optional[Dict[str, str]] = None,
) -> Optional[SchemaMapping]:
    if declared_schema_mapping:
        # If we are given a declared mapping, then that overrides a natural mapping
        return SchemaMapping(
            mapping=declared_schema_mapping,
            from_schema=data_block.realized_schema(env),
        )
    if expected_schema is None:
        # Nothing expected, so no mapping needed
        return None
    # Otherwise map found schema to expected schema
    return data_block.expected_schema(env).get_mapping_to(env, expected_schema)


#
# @dataclass
# class BoundPipeInterface:
#     inputs: List[NodeInput]
#     output: Optional[PipeAnnotation]
#     requires_pipe_context: bool = True
#     resolved_generics: Dict[str, ObjectSchemaKey] = field(default_factory=dict)
#     manually_set_resolved_output_schema: Optional[
#         ObjectSchema
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
#     def from_dfi(cls, dfi: PipeInterface) -> BoundPipeInterface:
#         return BoundPipeInterface(
#             inputs=[NodeInput(name=a.name, original_annotation=a) for a in dfi.inputs],
#             output=dfi.output,
#             requires_pipe_context=dfi.requires_pipe_context,
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
#     #             ctx, mapping=i.get_schema_mapping(ctx.env)
#     #         )
#     #     return inputs
#
#     def resolved_output_schema(self, env: Environment) -> Optional[ObjectSchema]:
#         if self.manually_set_resolved_output_schema is not None:
#             return self.manually_set_resolved_output_schema
#         if self.output is None:
#             return None
#         if self.output.is_generic:
#             k = self.resolved_generics[self.output.schema_like]
#             return env.get_schema(k)
#         return self.output.schema(env)
#
#     def set_resolved_output_schema(self, schema: ObjectSchema):
#         self.manually_set_resolved_output_schema = schema
#

#
#     def bind_and_specify_schemas(self, env: Environment, input_blocks: InputBlocks):
#         if self.is_bound:
#             raise Exception("Already bound")
#         realized_generics: Dict[str, ObjectSchema] = {}
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

    def get_bound_stream_interface(
        self, input_db_streams: Optional[InputStreams] = None
    ) -> BoundInterface:
        ci = self.get_connected_interface()
        if input_db_streams is None:
            input_db_streams = self.get_input_data_block_streams()
        return ci.bind(input_db_streams)

    def get_connected_interface(self) -> ConnectedInterface:
        inputs = self.node.get_declared_input_nodes()
        # Add "this" if it has a self-ref (TODO: a bit hidden down here no?)
        for annotation in self.pipe_interface.inputs:
            if annotation.is_self_ref:
                inputs["this"] = DeclaredNodeInput(
                    node=self.node,
                    declared_schema_mapping=self.node.get_schema_mapping_for_input(
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
        from dags.core.streams import ensure_data_stream_builder
        from dags.core.pipe import InputExhaustedException

        input_streams: InputStreams = {}
        any_unprocessed = False
        for input in self.get_connected_interface().inputs:
            node_or_stream = input.input_node
            assert node_or_stream is not None
            logger.debug(
                f"Getting input block for `{input.name}` from {node_or_stream}"
            )
            stream_builder = ensure_data_stream_builder(node_or_stream)
            stream_builder = self._build_stream(
                stream_builder,
                input,
                self.ctx.all_storages if self.strict_storages else None,
            )
            # if block is not None:
            #     logger.debug(f"Found: {block}")
            #     logger.debug(list(block.stored_data_blocks.all()))

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
                    # print(actual_input_node, annotation, storages)
                    raise InputExhaustedException(
                        f"    Required input '{input.name}'={stream_builder} to Pipe '{self.node.key}' is empty"
                    )
            else:
                try:
                    expected_schema = input.annotation.schema(self.env)
                except GenericSchemaException:
                    expected_schema = None
                input_streams[input.name] = stream_builder.as_managed_stream(
                    self.ctx,
                    expected_schema=expected_schema,
                    declared_schema_mapping=input.declared_schema_mapping,
                )
            any_unprocessed = True

        if input_streams and not any_unprocessed:
            raise InputExhaustedException("All inputs exhausted")

        return input_streams

    def _build_stream(
        self,
        stream_builder: DataBlockStreamBuilder,
        input: NodeInput,
        storages: List[Storage] = None,
    ) -> DataBlockStreamBuilder:
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
