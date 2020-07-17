from __future__ import annotations

import enum
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Union

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
    DataFunctionDefinition,
    DataFunctionLike,
    make_data_function,
    make_data_function_name,
)
from basis.core.data_function_interface import (
    SELF_REF_PARAM_NAME,
    DataFunctionInterface,
)
from basis.core.environment import Environment
from basis.core.metadata.orm import BaseModel

if TYPE_CHECKING:
    from basis.core.runnable import ExecutionContext
    from basis.core.streams import DataBlockStream


NodeLike = Union[str, "Node"]


class Node:
    env: Environment
    name: str
    data_function: DataFunction
    _raw_inputs: Dict[str, NodeLike]
    declared_composite_node_name: Optional[str]

    def __init__(
        self,
        env: Environment,
        name: str,
        data_function: DataFunctionLike,
        inputs: Optional[Union[NodeLike, Dict[str, NodeLike]]] = None,
        config: Dict[str, Any] = None,
        declared_composite_node_name: str = None,
    ):
        self.config = config or {}
        self.env = env
        self.name = name
        self.data_function = make_data_function(data_function)
        self.dfi = self.get_interface()
        self.declared_composite_node_name = declared_composite_node_name
        self._raw_inputs = {}
        if inputs is not None:
            self.set_inputs(inputs)

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name}, data_function={self.data_function.name})>"

    def __hash__(self):
        return hash(self.name)

    def set_inputs(self, inputs: Union[NodeLike, Dict[str, NodeLike]]):
        self._raw_inputs = self.dfi.assign_inputs(inputs)

    def clone(self) -> Node:
        return Node(
            env=self.env,
            name=self.name,
            data_function=self.data_function,
            config=self.config,
            inputs=self.get_raw_inputs(),
        )

    def get_dataset_node_name(self) -> str:
        return f"{self.name}__dataset"

    def get_dataset_node(self, node_name: str = None) -> Node:
        dfi = self.get_interface()
        if dfi.output is None:
            raise
        if dfi.output.data_format_class == "DataSet":
            df = "core.as_dataset"
        else:
            df = "core.accumulate_as_dataset"
        return Node(
            env=self.env,
            name=node_name or self.get_dataset_node_name(),
            data_function=df,
            config={"dataset_name": self.get_dataset_node_name()},
            inputs=self,
        )

    def _get_interface(self) -> Optional[DataFunctionInterface]:
        return self.data_function.get_interface()

    def get_interface(self) -> DataFunctionInterface:
        dfi = self._get_interface()
        if dfi is None:
            raise TypeError(f"DFI is none for {self}")
        # up = self.get_upstream()
        # if up is not None:
        #     dfi.connect_upstream(self, up)
        return dfi

    def get_inputs(self, env: Environment) -> Dict[str, Node]:
        return {name: env.get_node(dnl) for name, dnl in self.get_raw_inputs().items()}

    def get_raw_inputs(self) -> Dict[str, NodeLike]:
        return self._raw_inputs or {}

    def make_sub_nodes(self) -> Iterable[Node]:
        return build_composite_nodes(self)

    def is_composite(self) -> bool:
        return self.data_function.is_composite

    def as_stream(self) -> DataBlockStream:
        from basis.core.streams import DataBlockStream

        return DataBlockStream(upstream=self)

    def get_latest_output(self, ctx: ExecutionContext) -> Optional[DataBlock]:
        block = (
            ctx.metadata_session.query(DataBlockMetadata)
            .join(DataBlockLog)
            .join(DataFunctionLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                DataFunctionLog.node_name == self.name,
            )
            .order_by(DataBlockLog.created_at.desc())
            .first()
        )
        if block is None:
            return None
        return block.as_managed_data_block(ctx)


def ensure_data_function(
    self, env: Environment, df_like: Union[DataFunctionLike, str]
) -> DataFunction:
    if isinstance(df_like, DataFunction):
        return df_like
    if isinstance(df_like, DataFunctionDefinition):
        return df_like.as_data_function()
    if isinstance(df_like, str) or isinstance(df_like, ComponentUri):
        return env.get_function(df_like)
    return make_data_function(df_like)


def build_composite_nodes(n: Node) -> Iterable[Node]:
    if not n.data_function.is_composite:
        raise
    nodes = []
    # TODO: just supports list for now
    input_node = n
    for fn in n.data_function.get_representative_definition().sub_graph:
        fn = ensure_data_function(n.env, fn)
        child_fn_name = make_data_function_name(fn)
        child_node_name = f"{n.name}__{child_fn_name}"
        node = Node(
            n.env,
            child_node_name,
            fn,
            config=n.config,
            inputs=input_node,
            declared_composite_node_name=n.declared_composite_node_name
            or n.name,  # Handle nested composite functions
        )
        nodes.append(node)
        input_node = node
    return nodes

    # @property
    # def uri(self):
    #     return self.name  # TODO


class DataFunctionLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_name = Column(String, nullable=False)
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
            node_name=self.node_name,
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
