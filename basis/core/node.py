from __future__ import annotations

import enum
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from sqlalchemy.orm import relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import JSON, DateTime, Enum, Integer, String

from basis.core.data_block import DataBlock, DataBlockMetadata
from basis.core.data_function import (
    DataFunction,
    DataFunctionLike,
    ensure_data_function,
    make_data_function,
    make_data_function_name,
)
from basis.core.data_function_interface import DataFunctionInterface
from basis.core.environment import Environment
from basis.core.metadata.orm import BaseModel

if TYPE_CHECKING:
    from basis.core.runnable import ExecutionContext
    from basis.core.streams import DataBlockStream


NodeLike = Union[str, "Node"]


def inputs_as_nodes(env: Environment, inputs: Dict[str, NodeLike]):
    return {name: env.get_node(dnl) for name, dnl in inputs.items()}


class Node:
    env: Environment
    name: str
    data_function: DataFunction
    _declared_inputs: Dict[str, NodeLike]
    _compiled_inputs: Dict[str, Node] = None
    declared_composite_node_name: Optional[str] = None
    _sub_nodes: List[Node] = None

    def __init__(
        self,
        env: Environment,
        name: str,
        data_function: Union[DataFunctionLike, str],
        inputs: Optional[Union[NodeLike, Dict[str, NodeLike]]] = None,
        config: Dict[str, Any] = None,
        declared_composite_node_name: str = None,
    ):
        self.config = config or {}
        self.env = env
        self.name = name
        self.data_function = self._clean_data_function(data_function)
        self.dfi = self.get_interface()
        self.declared_composite_node_name = declared_composite_node_name
        self._declared_inputs = {}
        if inputs is not None:
            self._set_declared_inputs(inputs)
        if self.is_composite():
            self._sub_nodes = list(build_composite_nodes(self))

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name}, data_function={self.data_function.name})>"

    def __hash__(self):
        return hash(self.name)

    def _clean_data_function(self, df: DataFunctionLike) -> DataFunction:
        if isinstance(df, str):
            return self.env.get_function(df)
        return make_data_function(df)

    def _set_declared_inputs(self, inputs: Union[NodeLike, Dict[str, NodeLike]]):
        self._declared_inputs = self.dfi.assign_inputs(inputs)

    def set_compiled_inputs(self, inputs: Dict[str, NodeLike]):
        self._compiled_inputs = inputs_as_nodes(self.env, inputs)
        if self.is_composite():
            self.get_input_node().set_compiled_inputs(inputs)

    # def clone(self, new_name: str, **kwargs) -> Node:
    #     args = dict(
    #         env=self.env,
    #         name=new_name,
    #         data_function=self.data_function,
    #         config=self.config,
    #         inputs=self.get_declared_inputs(),
    #     )
    #     args.update(kwargs)
    #     return Node(**args)

    def get_dataset_node_name(self) -> str:
        return f"{self.name}__dataset"

    def get_dataset_node(self, node_name: str = None) -> Node:
        try:
            # Return if already created
            return self.env.get_node(self.get_dataset_node_name())
        except KeyError:
            pass
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
        return self.data_function.get_interface(self.env)

    def get_interface(self) -> DataFunctionInterface:
        dfi = self._get_interface()
        if dfi is None:
            raise TypeError(f"DFI is none for {self}")
        # up = self.get_upstream()
        # if up is not None:
        #     dfi.connect_upstream(self, up)
        return dfi

    def get_compiled_input_nodes(self) -> Dict[str, Node]:
        # TODO: mark compiled?
        # if self._compiled_inputs is None:
        #     raise Exception("Node has not been compiled")
        return self._compiled_inputs or self.get_declared_input_nodes()

    def get_declared_input_nodes(self) -> Dict[str, Node]:
        return inputs_as_nodes(self.env, self.get_declared_inputs())

    def get_declared_inputs(self) -> Dict[str, NodeLike]:
        return self._declared_inputs or {}

    def get_compiled_input_names(self) -> Dict[str, str]:
        return {
            n: i.name if isinstance(i, Node) else i
            for n, i in self.get_compiled_input_nodes().items()
        }

    def get_sub_nodes(self) -> Iterable[Node]:
        return self._sub_nodes

    def get_output_node(self) -> Node:
        if self.is_composite():
            return self._sub_nodes[-1].get_output_node()
        return self

    def get_input_node(self) -> Node:
        if self.is_composite():
            return self._sub_nodes[0].get_input_node()
        return self

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


def build_composite_nodes(n: Node) -> Iterable[Node]:
    if not n.data_function.is_composite:
        raise
    nodes = []
    # TODO: just supports list for now
    raw_inputs = list(n.get_declared_inputs().values())
    assert len(raw_inputs) == 1, "Composite functions take one input"
    input_node = raw_inputs[0]
    for fn in n.data_function.get_representative_definition().sub_graph:
        fn = ensure_data_function(n.env, fn)
        child_fn_name = make_data_function_name(fn)
        child_node_name = f"{n.name}__{child_fn_name}"
        try:
            node = n.env.get_node(child_node_name)
        except KeyError:
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
