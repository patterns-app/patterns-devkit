from __future__ import annotations

import enum
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Union

from pandas import DataFrame
from sqlalchemy.orm import relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import JSON, DateTime, Enum, Integer, String

from basis.core.component import ComponentUri
from basis.core.data_block import (
    DataBlock,
    DataBlockMetadata,
    create_data_block_from_records,
)
from basis.core.data_function import (
    DataFunction,
    DataFunctionCallable,
    DataFunctionDefinition,
    DataFunctionLike,
    ensure_datafunction,
    ensure_datafunction_definition,
    make_datafunction_name,
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
        ensure_data_stream,
        FunctionNodeRawInput,
        InputStreams,
    )


class FunctionNode:
    env: Environment
    name: str
    datafunction: DataFunction
    _upstream: Optional[InputStreams]
    parent_node: Optional[FunctionNode]

    def __init__(
        self,
        _env: Environment,
        _name: str,
        _datafunction: DataFunctionLike,
        upstream: Optional[FunctionNodeRawInput] = None,
        config: Dict[str, Any] = None,
    ):
        # if inputs:
        #     if upstream:
        #         raise Exception("Specify `upstream` OR kwarg inputs, not both")
        #     upstream = inputs
        self.config = config or {}
        self.env = _env
        self.name = _name
        self.datafunction = ensure_datafunction(_datafunction)
        self.set_upstream(upstream)
        self._children: Set[FunctionNode] = set([])
        self.parent_node = None

    def __repr__(self):
        name = self.datafunction.name
        return f"<{self.__class__.__name__}(name={self.name}, datafunction={name})>"

    def __hash__(self):
        return hash(self.name)

    # @property
    # def uri(self):
    #     return self.name  # TODO

    def set_upstream(self, upstream: FunctionNodeRawInput):
        self._upstream = self.check_datafunction_inputs(upstream)

    def add_upstream(self, upstream: FunctionNodeRawInput):
        # TODO: how to add/merge another upstream (useful for incr. building graphs when you don't know all inputs at once)
        raise NotImplementedError

    def get_upstream(self) -> Optional[InputStreams]:
        return self._upstream

    def check_datafunction_inputs(
        self, upstream: Optional[FunctionNodeRawInput]
    ) -> Optional[InputStreams]:
        """
        validate resource v set, and validate types
        """
        # return process_function_node_raw_input(upstream)
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

    def _get_interface(self) -> Optional[DataFunctionInterface]:
        return self.datafunction.get_interface()

    def get_interface(self) -> DataFunctionInterface:
        dfi = self._get_interface()
        if dfi is None:
            raise TypeError(f"DFI is none for {self}")
        up = self.get_upstream()
        if up is not None:
            dfi.connect_upstream(self, up)
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
                DataFunctionLog.function_node_name == self.name,
            )
            .order_by(DataBlockLog.created_at.desc())
            .first()
        )
        if block is None:
            return None
        return block.as_managed_data_block(ctx)


class DataFunctionLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    function_node_name = Column(String, nullable=False)
    data_function_uri = Column(String, nullable=False)
    data_function_config = Column(JSON, nullable=True)
    runtime_url = Column(String, nullable=False)
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error = Column(JSON, nullable=True)
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="data_function_log"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            function_node_name=self.function_node_name,
            data_function_uri=self.data_function_uri,
            runtime_url=self.runtime_url,
            started_at=self.started_at,
        )

    def output_data_blocks(self) -> Iterable[DataBlockMetadata]:
        return [db for db in self.data_block_logs if db.direction == Direction.OUTPUT]

    def input_data_blocks(self) -> Iterable[DataBlockMetadata]:
        return [db for db in self.data_block_logs if db.direction == Direction.INPUT]

    def set_error(self, e: Exception):
        tback = traceback.format_exc()
        # Traceback can be v large (like in max recursion), so we truncate to 5k chars
        self.error = {"error": str(e), "traceback": tback[:5000]}


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
    direction = Column(Enum(Direction, native_enum=False), nullable=False)
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
    # TODO: Idea here is to ensure inheriting classes are making names correctly. maybe not necessary
    # def validate_nodes(self):
    #     for node in self._internal_nodes:
    #         if node is self.input_node:
    #             continue
    #         if not node.name.startswith(self.get_node_name_prefix()):
    #             raise Exception("Child-node does not have parent name prefix")

    def make_child_name(self, parent_name: str, child_name: str) -> str:
        return f"{parent_name}__{child_name}"

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

    def ensure_data_function(
        self, df_like: Union[DataFunctionLike, str]
    ) -> DataFunction:
        if isinstance(df_like, DataFunction):
            return df_like
        if isinstance(df_like, DataFunctionDefinition):
            return df_like.as_data_function()
        if isinstance(df_like, str) or isinstance(df_like, ComponentUri):
            return self.env.get_function(df_like)
        return ensure_datafunction(df_like)


class FunctionNodeChain(CompositeFunctionNode):
    def __init__(
        self,
        _env: Environment,
        _name: str,
        _datafunction: DataFunction,
        upstream: Optional[FunctionNodeRawInput] = None,
        config: Dict[str, Any] = None,
    ):
        self.data_function_chain = ensure_datafunction(_datafunction)
        self._child_nodes = None
        super().__init__(_env, _name, _datafunction, upstream=upstream, config=config)

    def _build_nodes(self) -> List[FunctionNode]:
        from basis.core.streams import ensure_data_stream

        if self._child_nodes is not None:
            return self._child_nodes

        nodes = []
        for (
            fn
        ) in self.data_function_chain.get_representative_definition().sub_functions:
            fn = self.ensure_data_function(fn)
            child_name = make_datafunction_name(fn)
            child_name = self.make_child_name(self.name, child_name)
            # try:
            #     node = self.env.get_node(child_name)
            # except KeyError:
            print(f"creating {child_name}")
            node = FunctionNode(self.env, child_name, fn, config=self.config)
            node.parent_node = self
            nodes.append(node)
        self._child_nodes = nodes
        return nodes

    def set_upstream(self, upstream: FunctionNodeRawInput):
        from basis.core.streams import ensure_data_stream

        super().set_upstream(upstream)
        upstream = super().get_upstream()
        nodes = self.get_nodes()
        for fn in nodes:
            print(f"setting node {fn.name} upstream {upstream}")
            fn.set_upstream(upstream)
            upstream = ensure_data_stream(fn)

    def get_nodes(self) -> List[FunctionNode]:
        return self._build_nodes()


def is_composite_function_node(df_like: Any) -> bool:
    return isinstance(df_like, CompositeFunctionNode)


def function_node_factory(
    env: Environment,
    name: str,
    df_like: Any,
    upstream: Optional[FunctionNodeRawInput] = None,
    config: Dict[str, Any] = None,
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
        # TODO: other composites besides chains, and is there now a principled way to do this without the factory?
        #      like a DFD class has a FN factory method? Yes
        node = FunctionNodeChain(env, name, df, upstream=upstream, config=config)
    else:
        node = FunctionNode(env, name, df, upstream=upstream, config=config)
    return node


# def process_function_node_raw_input(
#     ctx: ExecutionContext, upstream: FunctionNodeRawInput
# ) -> Optional[InputStreams]:
#     from basis.core.streams import DataBlockStream, ensure_data_stream
#
#     if upstream is None:
#         return None
#     if isinstance(upstream, FunctionNode) or isinstance(upstream, DataBlockStream):
#         return ensure_data_stream(upstream)
#     if isinstance(upstream, str):
#         return ensure_data_stream(ctx.env.get_node(upstream))
#     # TODO: nope, check for dict first, then process each value in this function
#     if isinstance(upstream, dict):
#         return {
#             k: ensure_data_stream(ctx.env.get_node(v) if isinstance(v, str) else v)
#             for k, v in upstream.items()
#         }
#     # if isinstance(upstream, DataFrame) or isinstance(
#     #     upstream, list
#     # ):  # DataFrame or RecordsList
#     #     block, sdb = create_data_block_from_records(
#     #         ctx.env, ctx.metadata_session, ctx.local_memory_storage, upstream
#     #     )
#     #     assert block.id
#     #     return DataBlockStream(data_block=block.id)
#     raise Exception(f"Invalid data function input {upstream}")
