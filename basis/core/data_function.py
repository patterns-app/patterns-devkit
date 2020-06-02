from __future__ import annotations

import enum
import inspect
import re
from dataclasses import asdict, dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Union

from pandas import DataFrame
from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, String, func
from sqlalchemy.orm import RelationshipProperty, relationship

from basis.core.data_block import DataBlock, DataBlockMetadata, DataSetMetadata
from basis.core.data_format import DatabaseTable, DictList
from basis.core.data_function_interface import (
    SELF_REF_PARAM_NAME,
    DataFunctionInterface,
    ResolvedFunctionNodeInput,
)
from basis.core.environment import Environment
from basis.core.metadata.orm import BaseModel
from basis.core.typing.object_type import (
    ObjectType,
    ObjectTypeLike,
    ObjectTypeUri,
    is_generic,
)
from basis.utils.common import printd

if TYPE_CHECKING:
    from basis.core.storage import Storage
    from basis.core.runnable import DataFunctionContext, ExecutionContext
    from basis.core.streams import (
        InputBlocks,
        DataBlockStream,
        DataBlockStreamable,
        ensure_data_stream,
        FunctionNodeRawInput,
        FunctionNodeInput,
        InputStreams,
    )


# re_type_hint = re.compile(
#     r"(?P<iterable>(Iterator|Iterable|Sequence|List)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
# )
# re_type_hint = re.compile(
#     r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
# )
#
#
# VALID_DATA_INTERFACE_TYPES = [
#     "DataBlock",
#     "DataSet",
#     "DataFrame",
#     "DictList",
#     "DictListIterator",
#     "DatabaseTable",
#     # TODO: is this list just a list of formats? which ones are valid i/o to DFs?
#     # TODO: also, are DataBlocks the only valid *input* type?
#     # "DatabaseCursor",
# ]
#
# SELF_REF_PARAM_NAME = "this"
#
#
# @dataclass
# class DataFunctionAnnotation:
#     data_format_class: str
#     otype_like: ObjectTypeLike
#     name: Optional[str] = None
#     connected_stream: Optional[DataBlockStream] = None
#     resolved_otype: Optional[ObjectType] = None
#     bound_data_block: Optional[DataBlockMetadata] = None
#     is_iterable: bool = False  # TODO: what is state of iterable support?
#     is_variadic: bool = False  # TODO: what is state of variadic support?
#     is_generic: bool = False
#     is_optional: bool = False
#     is_self_ref: bool = False
#     original_annotation: Optional[str] = None
#
#     @classmethod
#     def create(cls, **kwargs) -> DataFunctionAnnotation:
#         name = kwargs.get("name")
#         if name:
#             kwargs["is_self_ref"] = name == SELF_REF_PARAM_NAME
#         otype_key = kwargs.get("otype_like")
#         if isinstance(otype_key, str):
#             kwargs["is_generic"] = is_generic(otype_key)
#         if kwargs["data_format_class"] not in VALID_DATA_INTERFACE_TYPES:
#             raise TypeError(
#                 f"`{kwargs['data_format_class']}` is not a valid data input type"
#             )
#         return DataFunctionAnnotation(**kwargs)
#
#     @classmethod
#     def from_parameter(cls, parameter: inspect.Parameter) -> DataFunctionAnnotation:
#         annotation = parameter.annotation
#         is_optional = parameter.default != inspect.Parameter.empty
#         is_variadic = parameter.kind == inspect.Parameter.VAR_POSITIONAL
#         tda = cls.from_type_annotation(
#             annotation,
#             name=parameter.name,
#             is_optional=is_optional,
#             is_variadic=is_variadic,
#         )
#         return tda
#
#     @classmethod
#     def from_type_annotation(cls, annotation: str, **kwargs) -> DataFunctionAnnotation:
#         """
#         Annotation of form `DataBlock[T]` for example
#         """
#         m = re_type_hint.match(annotation)
#         if m is None:
#             raise Exception(f"Invalid DataFunction annotation '{annotation}'")
#         is_optional = bool(m.groupdict()["optional"])
#         data_format_class = m.groupdict()["origin"]
#         otype_key = m.groupdict()["arg"]
#         args = dict(
#             data_format_class=data_format_class,
#             otype_like=otype_key,
#             is_optional=is_optional,
#             original_annotation=annotation,
#         )
#         args.update(**kwargs)
#         return DataFunctionAnnotation.create(**args)  # type: ignore
#
#     def otype_uri(self, env: Environment) -> ObjectTypeUri:
#         if self.is_generic:
#             raise  # TODO: ?? is this really an error? What is the URI of a generic otype?
#         return env.get_otype(self.otype_like).uri
#
#
# @dataclass
# class ResolvedFunctionNodeInput:
#     name: str
#     connected_stream: DataBlockStream
#     parent_nodes: List[FunctionNode]
#     resolved_otype: ObjectType
#     bound_data_block: Optional[DataBlockMetadata] = None
#
#
# @dataclass
# class ResolvedFunctionInterface:
#     resolved_inputs: List[ResolvedFunctionNodeInput]
#     resolved_output_otype: ObjectType
#     requires_data_function_context: bool = True
#     is_bound: bool = False
#
#     def get_input(self, name: str) -> ResolvedFunctionNodeInput:
#         for input in self.resolved_inputs:
#             if input.name == name:
#                 return input
#         raise KeyError(name)
#
#     def bind_data_blocks(self, input_blocks: InputBlocks):
#         if self.is_bound:
#             raise Exception("Already bound")
#         for name, input_block in input_blocks.items():
#             self.get_input(name).bound_data_block = input_block
#         self.is_bound = True
#
#     def as_kwargs(self):
#         if not self.is_bound:
#             raise Exception("Interface not bound")
#         return {i.name: i.bound_data_block for i in self.resolved_inputs}
#
#
# @dataclass
# class DataFunctionInterface:
#     inputs: List[DataFunctionAnnotation]
#     output: Optional[DataFunctionAnnotation]
#     requires_data_function_context: bool = True
#     # is_connected: bool = False
#     # is_resolved: bool = False
#     # is_bound: bool = False
#
#     @classmethod
#     def from_datafunction_definition(
#         cls, df: DataFunctionCallable
#     ) -> DataFunctionInterface:
#         requires_context = False
#         signature = inspect.signature(df)
#         output = None
#         ret = signature.return_annotation
#         if ret is not inspect.Signature.empty:
#             if not isinstance(ret, str):
#                 raise Exception("Return type annotation not a string")
#             output = DataFunctionAnnotation.from_type_annotation(ret)
#         inputs = []
#         for name, param in signature.parameters.items():
#             a = param.annotation
#             if a is not inspect.Signature.empty:
#                 if not isinstance(a, str):
#                     raise Exception("Parameter type annotation not a string")
#             try:
#                 a = DataFunctionAnnotation.from_parameter(param)
#                 inputs.append(a)
#             except TypeError:
#                 # Not a DataBlock/Set
#                 if param.annotation == "DataFunctionContext":
#                     requires_context = True
#                 else:
#                     raise Exception(f"Invalid data function parameter {param}")
#         dfi = DataFunctionInterface(
#             inputs=inputs,
#             output=output,
#             requires_data_function_context=requires_context,
#         )
#         dfi.validate_inputs()  # TODO: let caller handle this?
#         return dfi
#
#     def get_input(self, name: str) -> DataFunctionAnnotation:
#         for input in self.inputs:
#             if input.name == name:
#                 return input
#         raise KeyError(name)
#
#     def get_non_recursive_inputs(self):
#         return [i for i in self.inputs if not i.is_self_ref]
#
#     def get_inputs_dict(self) -> Dict[str, DataFunctionAnnotation]:
#         return {i.name: i for i in self.inputs if i.name}
#
#     def match_node_to_input(self, node: DataBlockStreamable) -> DataFunctionAnnotation:
#         if len(self.inputs) == 1:
#             return self.inputs[0]
#         # TODO: match ambiguous inputs
#         raise NotImplementedError(
#             "Ambiguous inputs. Give explicit keyword inputs to DataFunction"
#         )
#
#     def connect_upstream(self, upstream: FunctionNodeInput):
#         from basis.core.streams import DataBlockStream
#
#         if self.is_connected:
#             raise Exception("Already connected")
#         if isinstance(upstream, FunctionNode) or isinstance(upstream, DataBlockStream):
#             upstream = [upstream]
#         if isinstance(upstream, list):
#             print(self)
#             assert len(upstream) == len(
#                 self.get_non_recursive_inputs()
#             ), f"Wrong number of inputs. (Variadic inputs not supported yet) {upstream}"
#             for node in upstream:
#                 dfa = self.match_node_to_input(node)
#                 dfa.connected_stream = node
#         if isinstance(upstream, dict):
#             for name, node in upstream.items():
#                 self.get_input(name).connected_stream = node
#         self.is_connected = True
#
#     def resolve_otypes(self, env: Environment):
#         from basis.core.streams import DataBlockStream
#
#         if self.is_resolved:
#             raise Exception("Already resolved")
#         resolved_generics = {}
#         for input in self.inputs:
#             if input.is_generic:
#                 if not input.connected_stream:
#                     if input.is_optional:
#                         continue
#                     raise Exception(
#                         f"Input '{input.name}' not connected to upstream, cannot resolve"
#                     )
#                 if isinstance(input.connected_stream, DataBlockStream):
#                     raise NotImplementedError  # TODO: stream support
#                 upstream_rdfi = input.connected_stream.get_resolved_interface()
#                 if not upstream_rdfi.output:
#                     raise Exception("Upstream has no output, cannot resolve generic")
#                 resolved_otype = upstream_rdfi.output.resolved_otype
#                 resolved_generics[input.otype_like] = resolved_otype
#                 input.resolved_otype = resolved_otype
#             else:
#                 input.resolved_otype = env.get_otype(input.otype_like)
#         if self.output:
#             if self.output.is_generic:
#                 if self.output.otype_like not in resolved_generics:
#                     raise Exception(
#                         f"Generic not referenced in inputs: {self.output.otype_like}"
#                     )
#                 self.output.resolved_otype = resolved_generics[self.output.otype_like]
#             else:
#                 self.output.resolved_otype = env.get_otype(self.output.otype_like)
#         self.is_resolved = True
#
#     def validate_inputs(self):
#         # TODO: review this validation. what do we want to check for? Is most stuff checked above in connect/resolve?
#         data_block_seen = False
#         for annotation in self.inputs:
#             if (
#                 annotation.data_format_class == "DataBlock"
#                 and not annotation.is_optional
#             ):
#                 if data_block_seen:
#                     raise Exception(
#                         "Only one uncorrelated DataBlock input allowed to a DataFunction."
#                         "Correlate the inputs or use a DataSet"
#                     )
#                 data_block_seen = True
#
#     def bind_data_blocks(self, input_blocks: InputBlocks):
#         if self.is_bound:
#             raise Exception("Already bound")
#         if not self.is_resolved:
#             raise Exception("Interface must be connected before resolution")
#         for name, input_block in input_blocks.items():
#             self.get_input(name).bound_data_block = input_block
#         self.is_bound = True
#
#     def as_kwargs(self):
#         if not self.is_bound:
#             raise Exception("Interface not bound")
#         return {i.name: i.bound_data_block for i in self.inputs}
#
#
# class DataFunctionInterfaceManager:
#     """
#     Responsible for finding and preparing input streams for a
#     ConfiguredDataFunction, including resolving associated generic types.
#     """
#
#     def __init__(
#         self, ctx: ExecutionContext, node: FunctionNode,
#     ):
#         self.env = ctx.env
#         self.ctx = ctx
#         self.node = node
#         self.dfi = self.node.get_interface()
#
#     def get_resolved_interface(self) -> DataFunctionInterface:
#         if not self.dfi.is_resolved:
#             self.dfi.resolve_otypes(self.env)
#         return self.dfi
#
#     def get_bound_interface(
#         self, input_data_blocks: Optional[InputBlocks] = None
#     ) -> DataFunctionInterface:
#         if not self.dfi.is_resolved:
#             self.dfi.resolve_otypes(self.env)
#         if input_data_blocks is None:
#             input_data_blocks = self.get_input_data_blocks()
#         if not self.dfi.is_bound:
#             self.dfi.bind_data_blocks(input_data_blocks)
#         return self.dfi
#
#     def is_input_required(self, annotation: DataFunctionAnnotation) -> bool:
#         if annotation.is_optional:
#             return False
#         # TODO: more complex logic? hmmmm
#         return True
#
#     def get_input_data_blocks(self) -> InputBlocks:
#         from basis.core.streams import ensure_data_stream
#
#         input_data_blocks: InputBlocks = {}
#         any_unprocessed = False
#         for annotation in self.dfi.inputs:
#             assert annotation.name is not None
#             stream = self.node.get_data_stream_input(annotation.name)
#             printd(f"Getting {annotation} for {stream}")
#             stream = ensure_data_stream(stream)
#             block: Optional[DataBlockMetadata] = self.get_input_data_block(
#                 stream, annotation, self.ctx.all_storages
#             )
#             printd("\tFound:", block)
#
#             """
#             Inputs are considered "Exhausted" if:
#             - Single DR stream (and zero or more DSs): no unprocessed DRs
#             - Multiple correlated DR streams: ANY stream has no unprocessed DRs
#             - One or more DSs: if ALL DS streams have no unprocessed
#
#             In other words, if ANY DR stream is empty, bail out. If ALL DS streams are empty, bail
#             """
#             if block is None:
#                 printd(
#                     f"Couldnt find eligible DataBlocks for input `{annotation.name}` from {stream}"
#                 )
#                 if not annotation.is_optional:
#                     # print(actual_input_node, annotation, storages)
#                     raise InputExhaustedException(
#                         f"    Required input '{annotation.name}'={stream} to DataFunction '{self.node.key}' is empty"
#                     )
#             else:
#                 input_data_blocks[annotation.name] = block
#             if annotation.data_format_class == "DataBlock":
#                 any_unprocessed = True
#             elif annotation.data_format_class == "DataSet":
#                 if block is not None:
#                     any_unprocessed = any_unprocessed or stream.is_unprocessed(
#                         self.ctx, block, self.node
#                     )
#             else:
#                 raise NotImplementedError
#
#         if input_data_blocks and not any_unprocessed:
#             raise InputExhaustedException("All inputs exhausted")
#
#         return input_data_blocks
#
#     def get_input_data_block(
#         self,
#         stream: DataBlockStream,
#         annotation: DataFunctionAnnotation,
#         storages: List[Storage] = None,
#     ) -> Optional[DataBlockMetadata]:
#         if not annotation.is_generic:
#             stream = stream.filter_otype(annotation.otype_like)
#         if storages:
#             stream = stream.filter_storages(storages)
#         block: Optional[DataBlockMetadata]
#         if annotation.data_format_class in ("DataBlock",):
#             stream = stream.filter_unprocessed(
#                 self.node, allow_cycle=annotation.is_self_ref
#             )
#             block = stream.get_next(self.ctx)
#         elif annotation.data_format_class == "DataSet":
#             stream = stream.filter_dataset()
#             block = stream.get_most_recent(self.ctx)
#             # TODO: someday probably pass in actual DataSet (not underlying DR) to function that asks
#             #   for it (might want to use `name`, for instance). and then just proxy
#             #   through to underlying DR
#         else:
#             raise NotImplementedError
#
#         return block
#


class DataFunctionException(Exception):
    pass


class InputExhaustedException(DataFunctionException):
    pass


DataFunctionCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame, DictList, DatabaseTable, DataBlockMetadata, DataSetMetadata
]  # TODO: also input...?


class DataFunction:
    def __init__(self, key: str = None):
        self._key = key

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        raise NotImplementedError

    @property
    def key(self):
        return self._key

    def get_interface(self) -> DataFunctionInterface:
        raise NotImplementedError


def make_datafunction_key(data_function: DataFunctionCallable) -> str:
    # TODO: something more principled / explicit?
    if hasattr(data_function, "key"):
        return data_function.key  # type: ignore
    if hasattr(data_function, "__name__"):
        return data_function.__name__
    if hasattr(data_function, "__class__"):
        return data_function.__class__.__name__
    raise Exception(f"Invalid DataFunction Key {data_function}")


class PythonDataFunction(DataFunction):
    def __init__(self, data_function: DataFunctionCallable, key: str = None):
        self.data_function = data_function
        if key is None:
            key = make_datafunction_key(data_function)
        super().__init__(key=key)

    def __getattr__(self, item):
        return getattr(self.data_function, item)

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.data_function(*args, **kwargs)

    def get_interface(self) -> DataFunctionInterface:
        return DataFunctionInterface.from_datafunction_definition(self.data_function)


def datafunction(df=None, *, key=None):
    if df is None:
        return partial(datafunction, key=key)
    return PythonDataFunction(df, key)


class CompositeDataFunction:
    def __init__(self, key):
        self._key = key

    @property
    def key(self):
        return self._key


class DataFunctionChain(CompositeDataFunction):
    def __init__(self, key: str, function_chain: List[DataFunctionCallable]):
        self.function_chain = function_chain
        # if key is None:
        #     key = self.make_key()
        super().__init__(key)

    # def make_key(self) -> str:
    #     return "_".join(f.key for f in self.function_chain)


DataFunctionLike = Union[DataFunctionCallable, DataFunction, CompositeDataFunction]


def ensure_datafunction(dfl: DataFunctionLike) -> DataFunction:
    if isinstance(dfl, CompositeDataFunction):
        return dfl
    if isinstance(dfl, DataFunction):
        return dfl
    return PythonDataFunction(dfl)


class FunctionNode:
    env: Environment
    key: str
    datafunction: DataFunction
    _upstream: Optional[InputStreams]
    parent_node: Optional[FunctionNode]
    # resolved_output_type: Optional[ObjectType] = None
    # resolved_inputs: Optional[List[ResolvedFunctionNodeInput]] = None

    def __init__(
        self,
        _env: Environment,
        _key: str,
        _datafunction: DataFunctionLike,
        upstream: Optional[FunctionNodeRawInput] = None,
        **inputs: DataBlockStreamable,
    ):
        if inputs:
            if upstream:
                raise Exception("Specify `upstream` OR kwarg inputs, not both")
            upstream = inputs  # type: ignore  # mypy misses this compatible subtype
        self.env = _env
        self.key = _key
        self.datafunction = ensure_datafunction(_datafunction)
        self._upstream = self.check_datafunction_inputs(upstream)
        self._children: Set[FunctionNode] = set([])
        self.parent_node = None
        # self.resolved_output_type = None
        # self.resolved_inputs = None

    def __repr__(self):
        name = self.datafunction.key
        return f"<{self.__class__.__name__}(key={self.key}, datafunction={name})>"

    def __hash__(self):
        return hash(self.key)

    # def register_child_node(self, child: ConfiguredDataFunction):
    #     self._children.add(child)
    #
    # def get_children_nodes(self) -> Set[ConfiguredDataFunction]:
    #     return self._children

    # @property
    # def uri(self):
    #     return self.key  # TODO

    def set_upstream(self, upstream: FunctionNodeRawInput):
        self._upstream = self.check_datafunction_inputs(upstream)

    def get_upstream(self) -> FunctionNodeInput:
        return self._upstream

    # def set_resolved_output_type(self, upstream: FunctionNodeRawInput):
    #     self._upstream = self.check_datafunction_inputs(upstream)
    #
    # def get_resolved_output_type(self) -> FunctionNodeInput:
    #     return self._upstream

    def check_datafunction_inputs(
        self, upstream: Optional[FunctionNodeRawInput]
    ) -> Optional[InputStreams]:
        """
        validate resource v set, and validate types
        """
        from basis.core.streams import DataBlockStream, ensure_data_stream

        if upstream is None:
            return None
        if isinstance(upstream, FunctionNode) or isinstance(upstream, DataBlockStream):
            return ensure_data_stream(upstream)
        if isinstance(upstream, str):
            return ensure_data_stream(self.env.get_node(upstream))
        if isinstance(upstream, dict):
            return {
                k: ensure_data_stream(self.env.get_node(v) if isinstance(v, str) else v)
                for k, v in upstream.items()
            }
        raise Exception(f"Invalid data function input {upstream}")

    # def get_resolved_interface(self):
    #     dfi = self.get_interface()
    #     dfi.connect_upstream(self._upstream)
    #     dfi.resolve_otypes(self.env)
    #     return dfi

    def _get_interface(self) -> DataFunctionInterface:
        if hasattr(self.datafunction, "get_interface"):
            return self.datafunction.get_interface()
        if callable(self.datafunction):
            return DataFunctionInterface.from_datafunction_definition(self.datafunction)
        raise Exception("No interface found for datafunction")

    def get_interface(self) -> DataFunctionInterface:
        dfi = self._get_interface()
        dfi.connect_upstream(self.get_upstream())
        return dfi

    def get_input(self, name: str) -> Any:
        if name == SELF_REF_PARAM_NAME:
            return self
        try:
            dfi = self.get_interface()
            return dfi.get_input(name).connected_stream
        except KeyError:
            raise Exception(f"Missing input {name}")  # TODO: exception cleanup

    def get_inputs(self) -> Dict:
        dfi = self.get_interface()
        return {i.name: i.connected_stream for i in dfi.inputs}

    def get_data_stream_input(self, name: str) -> DataBlockStreamable:
        if name == SELF_REF_PARAM_NAME:
            return self
        v = self.get_input(name)
        if not self.is_data_stream_input(v):
            raise TypeError("Not a DRS")  # TODO: Exception cleanup
        return v

    def is_data_stream_input(self, v: Any) -> bool:
        from basis.core.streams import DataBlockStream

        # TODO: ignores "this" special arg (a bug/feature that build_fn_graph DEPENDS on currently [don't want recursion there])
        if isinstance(v, FunctionNode):
            return True
        elif isinstance(v, DataBlockStream):
            return True
        # elif isinstance(v, Sequence):
        #     raise TypeError("Sequences are deprecated")
        #     if v and isinstance(v[0], ConfiguredDataFunction):
        #         return True
        # if v and isinstance(v[0], ConfiguredDataFunction):
        #     return True
        return False

    # def get_data_stream_inputs(
    #     self, env: Environment, as_streams=False
    # ) -> List[DataBlockStreamable]:
    #
    #     streams: List[DataBlockStreamable] = []
    #     for k, v in self.get_inputs().items():
    #         if self.is_data_stream_input(v):
    #             # TODO: sequence deprecated
    #             # if isinstance(v, Sequence):
    #             #     streams.extend(v)
    #             # else:
    #             streams.append(v)
    #     if as_streams:
    #         streams = [ensure_data_stream(v) for v in streams]
    #     return streams

    def get_output_node(self) -> FunctionNode:
        # Handle nested NODEs
        # TODO: why would it be a function of the (un-configured) self.datafunction? I don't think it is
        # if hasattr(self.datafunction, "get_output_node"):
        #     return self.datafunction.get_output_node()
        return self  # Base case is self, not a nested NODE

    def as_stream(self) -> DataBlockStream:
        from basis.core.streams import DataBlockStream

        node = self.get_output_node()
        return DataBlockStream(upstream=node)

    def is_composite(self) -> bool:
        return isinstance(self.datafunction, CompositeDataFunction)

    def get_nodes(self) -> List[FunctionNode]:
        return [self]

    def get_latest_output(self, ctx: ExecutionContext) -> Optional[DataBlock]:
        block = (
            ctx.metadata_session.query(DataBlockMetadata)
            .join(DataBlockLog)
            .join(DataFunctionLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                DataFunctionLog.function_node_key == self.key,
            )
            .order_by(DataBlockLog.created_at.desc())
            .first()
        )
        if block is None:
            return None
        return block.as_managed_data_block(ctx)


class DataFunctionLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    function_node_key = Column(String, nullable=False)
    runtime_url = Column(String, nullable=False)
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="data_function_log"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            function_node_key=self.function_node_key,
            runtime_url=self.runtime_url,
            started_at=self.started_at,
        )


class Direction(enum.Enum):
    INPUT = "input"
    OUTPUT = "output"

    @property
    def symbol(self):
        if self.value == "input":
            return "←"
        return "➞"

    @property
    def display(self):
        s = "out"
        if self.value == "input":
            s = "in"
        return self.symbol + " " + s


class DataBlockLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    data_function_log_id = Column(
        Integer, ForeignKey(DataFunctionLog.id), nullable=False
    )
    data_block_id = Column(
        String, ForeignKey("basis_data_block_metadata.id"), nullable=False
    )  # TODO table name ref ugly here. We can parameterize with orm constant at least, or tablename("DataBlock.id")
    direction = Column(Enum(Direction), nullable=False)
    processed_at = Column(DateTime, default=func.now(), nullable=False)
    # Hints
    data_block: "DataBlockMetadata"
    data_function_log: DataFunctionLog

    def __repr__(self):
        return self._repr(
            id=self.id,
            data_function_log=self.data_function_log,
            data_block=self.data_block,
            direction=self.direction,
            processed_at=self.processed_at,
        )


class CompositeFunctionNode(FunctionNode):
    def build_nodes(self, input: DataBlockStreamable = None) -> List[FunctionNode]:
        raise NotImplementedError

    # TODO: Idea here is to ensure inheriting classes are making keys correctly. maybe not necessary
    # def validate_nodes(self):
    #     for node in self._internal_nodes:
    #         if node is self.input_node:
    #             continue
    #         if not node.name.startswith(self.get_node_key_prefix()):
    #             raise Exception("Child-node does not have parent name prefix")

    def make_child_key(self, parent_key: str, child_name: str) -> str:
        return f"{parent_key}__{child_name}"

    def get_nodes(self) -> List[FunctionNode]:
        raise NotImplementedError

    def get_input_node(self) -> FunctionNode:
        # TODO: shouldn't this be input_nodeS ...?
        return self.get_nodes()[0]

    def get_output_node(self) -> FunctionNode:
        return self.get_nodes()[-1]

    def get_interface(self) -> DataFunctionInterface:
        input_interface = self.get_input_node().get_interface()
        return DataFunctionInterface(
            inputs=input_interface.inputs,
            output=self.get_output_node().get_interface().output,
            requires_data_function_context=input_interface.requires_data_function_context,
        )

    # def get_resolved_interface(self) -> DataFunctionInterface:
    #     resolved_dfis = []
    #     for node in self.get_nodes():
    #         dfi = node.get_resolved_interface()
    #         resolved_dfis.append(dfi)
    #     return DataFunctionInterface(
    #         inputs=resolved_dfis[0].inputs,
    #         output=resolved_dfis[-1].output,
    #         requires_data_function_context=resolved_dfis[
    #             0
    #         ].requires_data_function_context,
    #         is_connected=True,
    #         is_resolved=True,
    #     )


class FunctionNodeChain(CompositeFunctionNode):
    def __init__(
        self,
        _env: Environment,
        _key: str,
        _datafunction: DataFunctionChain,
        upstream: DataBlockStreamable = None,
        **inputs: DataBlockStreamable,
    ):
        if upstream is not None:
            super().__init__(_env, _key, _datafunction, upstream=upstream, **inputs)
        else:
            super().__init__(_env, _key, _datafunction, **inputs)
        self.data_function_chain = _datafunction
        self._node_chain = self.build_nodes(upstream)

    def build_nodes(self, upstream: DataBlockStreamable = None) -> List[FunctionNode]:
        nodes = []
        for fn in self.data_function_chain.function_chain:
            child_name = make_datafunction_key(fn)
            child_key = self.make_child_key(self.key, child_name)
            if upstream is not None:
                node = FunctionNode(self.env, child_key, fn, upstream=upstream)
            else:
                node = FunctionNode(self.env, child_key, fn)
            node.parent_node = self
            nodes.append(node)
            upstream = node
        return nodes

    def get_nodes(self) -> List[FunctionNode]:
        return self._node_chain


def is_composite_data_function(df_like: Any) -> bool:
    return isinstance(df_like, CompositeFunctionNode)


def function_node_factory(
    env: Environment,
    key: str,
    df_like: Any,
    upstream: Optional[FunctionNodeRawInput] = None,
    **inputs: DataBlockStreamable,
) -> FunctionNode:
    from basis.core.streams import DataBlockStream

    node: FunctionNode
    if isinstance(df_like, CompositeDataFunction):
        if isinstance(df_like, DataFunctionChain):
            # if upstream:
            #     if isinstance(upstream, list):
            #         if len(upstream) == 1:
            #             upstream = upstream[0]
            #     assert isinstance(upstream, FunctionNode) or isinstance(
            #         upstream, DataBlockStream
            #     ), f"Upstream must be a single Streamable in a Chain: {upstream}"
            node = FunctionNodeChain(env, key, df_like, upstream=upstream, **inputs)
        else:
            raise NotImplementedError
    else:
        node = FunctionNode(env, key, df_like, upstream=upstream, **inputs)
    return node
