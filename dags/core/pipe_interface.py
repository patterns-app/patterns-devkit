from __future__ import annotations

import inspect
import re
from dataclasses import asdict, dataclass
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

from dags.core.data_block import DataBlock, DataBlockMetadata, DataSetMetadata
from dags.core.environment import Environment
from dags.core.typing.object_schema import (
    ObjectSchema,
    ObjectSchemaKey,
    ObjectSchemaLike,
    is_any,
    is_generic,
)
from dags.utils.common import printd
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
        DataBlockStream,
        ensure_data_stream,
        PipeNodeInput,
        InputStreams,
    )


# re_type_hint = re.compile(
#     r"(?P<iterable>(Iterator|Iterable|Sequence|List)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
# )
re_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)

VALID_DATA_INTERFACE_TYPES = [
    "DataBlock",
    "DataSet",
    "DataFrame",
    "RecordsList",
    "RecordsListGenerator",
    "DataFrameGenerator",
    "DatabaseTableRef",
    # TODO: is this list just a list of formats? which ones are valid i/o to Pipes?
    # TODO: also, are DataBlocks and DataSets the only valid *input* types? A bit confusing to end user I think
    # "DatabaseCursor",
]

SELF_REF_PARAM_NAME = "this"


class BadAnnotationException(Exception):
    pass


@dataclass
class PipeAnnotation:
    data_format_class: str
    schema_like: ObjectSchemaLike
    name: Optional[str] = None
    # is_iterable: bool = False  # TODO: what is state of iterable support?
    is_variadic: bool = False  # TODO: what is state of variadic support?
    is_generic: bool = False
    is_optional: bool = False
    is_self_ref: bool = False
    original_annotation: Optional[str] = None
    input_node: Optional[Node] = None
    bound_data_block: Optional[DataBlockMetadata] = None

    @property
    def is_dataset(self) -> bool:
        return self.data_format_class == "DataSet"

    @classmethod
    def create(cls, **kwargs) -> PipeAnnotation:
        if not kwargs.get("schema_like"):
            kwargs["schema_like"] = "Any"
        name = kwargs.get("name")
        if name:
            kwargs["is_self_ref"] = name == SELF_REF_PARAM_NAME
        schema_name = kwargs.get("schema_like")
        if isinstance(schema_name, str):
            kwargs["is_generic"] = is_generic(schema_name)
        if kwargs["data_format_class"] not in VALID_DATA_INTERFACE_TYPES:
            raise TypeError(
                f"`{kwargs['data_format_class']}` is not a valid data input type"
            )
        return PipeAnnotation(**kwargs)

    @classmethod
    def from_parameter(cls, parameter: inspect.Parameter) -> PipeAnnotation:
        annotation = parameter.annotation
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

    def schema_key(self, env: Environment) -> ObjectSchemaKey:
        if self.is_generic:
            raise ValueError("Generic ObjectSchema has no name")
        return env.get_schema(self.schema_like).key


@dataclass
class NodeInput:
    name: str
    original_annotation: PipeAnnotation
    input_node: Optional[Node] = None
    bound_data_block: Optional[DataBlockMetadata] = None


@dataclass
class BoundPipeInterface:
    inputs: List[NodeInput]
    output: Optional[PipeAnnotation]
    requires_pipe_context: bool = True

    def get_input(self, name: str) -> NodeInput:
        for input in self.inputs:
            if input.name == name:
                return input
        raise KeyError(name)

    def connect(self, input_nodes: Dict[str, Node]):
        for name, input_node in input_nodes.items():
            i = self.get_input(name)
            i.input_node = input_node

    def bind(self, input_blocks: Dict[str, DataBlockMetadata]):
        for name, input_block in input_blocks.items():
            i = self.get_input(name)
            i.bound_data_block = input_block

    @classmethod
    def from_dfi(cls, dfi: PipeInterface) -> BoundPipeInterface:
        return BoundPipeInterface(
            inputs=[NodeInput(name=a.name, original_annotation=a) for a in dfi.inputs],
            output=dfi.output,
            requires_pipe_context=dfi.requires_pipe_context,
        )

    def as_kwargs(self):
        return {
            i.name: i.bound_data_block
            for i in self.inputs
            if i.bound_data_block is not None
        }


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

# @classmethod
# def from_pipe_inteface(cls, dfi: PipeInterface, input_blocks: InputBlocks) -> BoundPipeInterface:
#     inputs = []
#     for name, input in input_blocks.items():
#         i = dfi.get_input(name)
#
#
#     return BoundPipeInterface(
#         inputs=inputs,
#         output=dfi.output,
#         requires_pipe_context=dfi.requires_pipe_context,
#     )


@dataclass
class PipeInterface:
    inputs: List[PipeAnnotation]
    output: Optional[PipeAnnotation]
    requires_pipe_context: bool = True
    # is_bound: bool = False

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
        dfi = PipeInterface(
            inputs=inputs, output=output, requires_pipe_context=requires_context,
        )
        dfi.validate_inputs()  # TODO: let caller handle this?
        return dfi

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
        # TODO: review this validation. what do we want to check for?
        data_block_seen = False
        for annotation in self.inputs:
            if (
                annotation.data_format_class == "DataBlock"
                and not annotation.is_optional
            ):
                if data_block_seen:
                    raise Exception(
                        "Only one uncorrelated DataBlock input allowed to a Pipe."
                        "Correlate the inputs or use a DataSet"
                    )
                data_block_seen = True

    def assign_inputs(
        self, inputs: Union[NodeLike, Dict[str, NodeLike]]
    ) -> Dict[str, NodeLike]:
        if not isinstance(inputs, dict):
            assert (
                len(self.get_non_recursive_inputs()) == 1
            ), f"Wrong number of inputs. (Variadic inputs not supported yet) {inputs} {self.get_non_recursive_inputs()}"
            return {self.get_non_recursive_inputs()[0].name: inputs}
        assert (set(inputs.keys()) - {"this"}) == set(
            i.name for i in self.get_non_recursive_inputs()
        ), f"{inputs}  {self.get_non_recursive_inputs()}"
        return inputs


class NodeInterfaceManager:
    """
    Responsible for finding and preparing DataBlocks for input to a
    Node.
    """

    def __init__(self, ctx: ExecutionContext, node: Node, strict_storages: bool = True):
        self.env = ctx.env
        self.ctx = ctx
        self.node = node
        self.dfi = self.node.get_interface()
        self.strict_storages = (
            strict_storages  # Only pull datablocks from given storages
        )

    def get_bound_interface(
        self, input_data_blocks: Optional[InputBlocks] = None
    ) -> BoundPipeInterface:
        i = BoundPipeInterface.from_dfi(self.dfi)
        # TODO: dry (see below)
        inputs = self.node.get_compiled_input_nodes()
        for input in i.inputs:
            if input.original_annotation.is_self_ref:
                inputs["this"] = self.node
        i.connect(inputs)
        if input_data_blocks is None:
            input_data_blocks = self.get_input_data_blocks()
        i.bind(input_data_blocks)
        return i

    def get_connected_interface(self) -> BoundPipeInterface:
        i = BoundPipeInterface.from_dfi(self.dfi)
        inputs = self.node.get_compiled_input_nodes()
        for input in i.inputs:
            if input.original_annotation.is_self_ref:
                inputs["this"] = self.node
        i.connect(inputs)
        return i

    def is_input_required(self, annotation: PipeAnnotation) -> bool:
        if annotation.is_optional:
            return False
        return True

    def get_input_data_blocks(self) -> InputBlocks:
        from dags.core.streams import ensure_data_stream
        from dags.core.pipe import InputExhaustedException

        input_data_blocks: InputBlocks = {}
        any_unprocessed = False
        for input in self.get_connected_interface().inputs:
            node_or_stream = input.input_node
            assert node_or_stream is not None
            logger.debug(
                f"Getting input block for `{input.name}` from {node_or_stream}"
            )
            stream = ensure_data_stream(node_or_stream)
            block: Optional[DataBlockMetadata] = self.get_input_data_block(
                stream, input, self.ctx.all_storages if self.strict_storages else None
            )
            if block is not None:
                logger.debug(f"Found: {block}")
                logger.debug(list(block.stored_data_blocks.all()))

            """
            Inputs are considered "Exhausted" if:
            - Single DB stream (and zero or more DSs): no unprocessed DRs
            - Multiple correlated DB streams: ANY stream has no unprocessed DRs
            - One or more DSs: if ALL DS streams have no unprocessed

            In other words, if ANY DB stream is empty, bail out. If ALL DS streams are empty, bail
            """
            if block is None:
                logger.debug(
                    f"Couldnt find eligible DataBlocks for input `{input.name}` from {stream}"
                )
                if not input.original_annotation.is_optional:
                    # print(actual_input_node, annotation, storages)
                    raise InputExhaustedException(
                        f"    Required input '{input.name}'={stream} to Pipe '{self.node.key}' is empty"
                    )
            else:
                input_data_blocks[input.name] = block
            if input.original_annotation.data_format_class == "DataBlock":
                any_unprocessed = True
            elif input.original_annotation.data_format_class == "DataSet":
                if block is not None:
                    any_unprocessed = any_unprocessed or stream.is_unprocessed(
                        self.ctx, block, self.node
                    )
            else:
                raise NotImplementedError

        if input_data_blocks and not any_unprocessed:
            raise InputExhaustedException("All inputs exhausted")

        return input_data_blocks

    def get_input_data_block(
        self, stream: DataBlockStream, input: NodeInput, storages: List[Storage] = None,
    ) -> Optional[DataBlockMetadata]:
        logger.debug(f"{stream.get_count(self.ctx)} available DataBlocks")
        if storages:
            stream = stream.filter_storages(storages)
            logger.debug(
                f"{stream.get_count(self.ctx)} available DataBlocks in storages {storages}"
            )
        block: Optional[DataBlockMetadata]
        logger.debug(f"Finding unprocessed input for: {stream}")
        if input.original_annotation.data_format_class in ("DataBlock",):
            stream = stream.filter_unprocessed(
                self.node, allow_cycle=input.original_annotation.is_self_ref
            )
            logger.debug(f"{stream.get_count(self.ctx)} unprocessed DataBlocks")
            block = stream.get_next(self.ctx)
        elif input.original_annotation.data_format_class == "DataSet":
            logger.debug("Finding DataSet")
            stream = stream.filter_dataset()
            block = stream.get_most_recent(self.ctx)
            # TODO: someday probably pass in actual DataSet (not underlying DB) to pipe that asks
            #   for it (might want to use `name`, for instance). and then just proxy
            #   through to underlying DB
        else:
            raise NotImplementedError

        return block
