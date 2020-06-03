from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from sqlalchemy.orm import relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import DateTime, Enum, Integer, String

from basis.core.data_block import DataBlock, DataBlockMetadata
from basis.core.data_function import (
    DataFunction,
    DataFunctionLike,
    ensure_datafunction,
    make_datafunction_key,
)
from basis.core.data_function_interface import (
    SELF_REF_PARAM_NAME,
    DataFunctionInterface,
)
from basis.core.environment import Environment
from basis.core.metadata.orm import BaseModel

if TYPE_CHECKING:
    from basis.core.runnable import ExecutionContext
    from basis.core.streams import (
        DataBlockStream,
        DataBlockStreamable,
        ensure_data_stream,
        FunctionNodeRawInput,
        InputStreams,
        DataBlockStreamLike,
    )


class FunctionNode:
    env: Environment
    key: str
    datafunction: DataFunction
    _upstream: Optional[InputStreams]
    parent_node: Optional[FunctionNode]

    def __init__(
        self,
        _env: Environment,
        _key: str,
        _datafunction: DataFunctionLike,
        upstream: Optional[FunctionNodeRawInput] = None,
        **inputs: DataBlockStreamLike,
    ):
        if inputs:
            if upstream:
                raise Exception("Specify `upstream` OR kwarg inputs, not both")
            upstream = inputs
        self.env = _env
        self.key = _key
        self.datafunction = ensure_datafunction(_datafunction)
        self._upstream = self.check_datafunction_inputs(upstream)
        self._children: Set[FunctionNode] = set([])
        self.parent_node = None

    def __repr__(self):
        name = self.datafunction.key
        return f"<{self.__class__.__name__}(key={self.key}, datafunction={name})>"

    def __hash__(self):
        return hash(self.key)

    # @property
    # def uri(self):
    #     return self.key  # TODO

    def set_upstream(self, upstream: FunctionNodeRawInput):
        self._upstream = self.check_datafunction_inputs(upstream)

    def get_upstream(self) -> Optional[InputStreams]:
        return self._upstream

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

    def _get_interface(self) -> DataFunctionInterface:
        if hasattr(self.datafunction, "get_interface"):
            return self.datafunction.get_interface()
        if callable(self.datafunction):
            return DataFunctionInterface.from_datafunction_definition(self.datafunction)
        raise Exception("No interface found for datafunction")

    def get_interface(self) -> DataFunctionInterface:
        dfi = self._get_interface()
        dfi.connect_upstream(self, self.get_upstream())
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

    # def get_data_stream_input(self, name: str) -> DataBlockStreamable:
    #     if name == SELF_REF_PARAM_NAME:
    #         return self
    #     v = self.get_input(name)
    #     if not self.is_data_stream_input(v):
    #         raise TypeError("Not a DRS")  # TODO: Exception cleanup
    #     return v
    #
    # def is_data_stream_input(self, v: Any) -> bool:
    #     from basis.core.streams import DataBlockStream
    #
    #     # TODO: ignores "this" special arg (a bug/feature that build_fn_graph DEPENDS on currently [don't want recursion there])
    #     if isinstance(v, FunctionNode):
    #         return True
    #     elif isinstance(v, DataBlockStream):
    #         return True
    #     # elif isinstance(v, Sequence):
    #     #     raise TypeError("Sequences are deprecated")
    #     #     if v and isinstance(v[0], ConfiguredDataFunction):
    #     #         return True
    #     # if v and isinstance(v[0], ConfiguredDataFunction):
    #     #     return True
    #     return False

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
        return self.datafunction.is_composite

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
    def build_nodes(self, upstream: InputStreams = None) -> List[FunctionNode]:
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

    def _get_interface(self) -> DataFunctionInterface:
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
        _datafunction: DataFunction,
        upstream: Optional[FunctionNodeRawInput] = None,
        **inputs: DataBlockStreamable,
    ):
        super().__init__(_env, _key, _datafunction, upstream=upstream, **inputs)
        self.data_function_chain = _datafunction
        input_streams = self.get_upstream()
        self._node_chain = self.build_nodes(input_streams)

    def build_nodes(self, upstream: InputStreams = None) -> List[FunctionNode]:
        from basis.core.streams import ensure_data_stream

        nodes = []
        for fn in self.data_function_chain.sub_functions:
            child_name = make_datafunction_key(fn)
            child_key = self.make_child_key(self.key, child_name)
            if upstream is not None:
                node = FunctionNode(self.env, child_key, fn, upstream=upstream)  # type: ignore  # mypy misses compatible subtype?
            else:
                node = FunctionNode(self.env, child_key, fn)
            node.parent_node = self
            nodes.append(node)
            upstream = ensure_data_stream(node)
        return nodes

    def get_nodes(self) -> List[FunctionNode]:
        return self._node_chain


def is_composite_function_node(df_like: Any) -> bool:
    return isinstance(df_like, CompositeFunctionNode)


def function_node_factory(
    env: Environment,
    key: str,
    df_like: Any,
    upstream: Optional[FunctionNodeRawInput] = None,
    **inputs: DataBlockStreamable,
) -> FunctionNode:
    node: FunctionNode
    df = ensure_datafunction(df_like)
    if df.is_composite:
        # if upstream:
        #     if isinstance(upstream, list):
        #         if len(upstream) == 1:
        #             upstream = upstream[0]
        #     assert isinstance(upstream, FunctionNode) or isinstance(
        #         upstream, DataBlockStream
        #     ), f"Upstream must be a single Streamable in a Chain: {upstream}"
        # TODO: other composites besides chains
        node = FunctionNodeChain(env, key, df, upstream=upstream, **inputs)
    else:
        node = FunctionNode(env, key, df, upstream=upstream, **inputs)
    return node
