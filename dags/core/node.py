from __future__ import annotations

import enum
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import JSON, DateTime, Enum, Integer, String

from dags.core.data_block import DataBlock, DataBlockMetadata
from dags.core.environment import Environment
from dags.core.metadata.orm import BaseModel
from dags.core.pipe import Pipe, PipeLike, ensure_pipe, make_pipe, make_pipe_key
from dags.core.pipe_interface import PipeInterface
from loguru import logger

if TYPE_CHECKING:
    from dags.core.runnable import ExecutionContext
    from dags.core.streams import DataBlockStream


NodeLike = Union[str, "Node"]


def inputs_as_nodes(env: Environment, inputs: Dict[str, NodeLike]):
    return {name: env.get_node(dnl) for name, dnl in inputs.items()}


class Node:
    env: Environment
    name: str
    pipe: Pipe
    _declared_inputs: Dict[str, NodeLike]
    _compiled_inputs: Dict[str, Node] = None
    _dataset_name: Optional[str] = None
    declared_composite_node_name: Optional[str] = None
    _sub_nodes: List[Node] = None

    def __init__(
        self,
        env: Environment,
        name: str,
        pipe: Union[PipeLike, str],
        inputs: Optional[Union[NodeLike, Dict[str, NodeLike]]] = None,
        dataset_name: Optional[str] = None,
        config: Dict[str, Any] = None,
        declared_composite_node_name: str = None,
    ):
        self.config = config or {}
        self.env = env
        self.name = name
        self._dataset_name = dataset_name
        self.pipe = self._clean_pipe(pipe)
        self.dfi = self.get_interface()
        self.declared_composite_node_name = declared_composite_node_name
        self._declared_inputs = {}
        if inputs is not None:
            self._set_declared_inputs(inputs)
        if self.is_composite():
            self._sub_nodes = list(build_composite_nodes(self))

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name}, pipe={self.pipe.name})>"

    def __hash__(self):
        return hash(self.name)

    def _clean_pipe(self, df: PipeLike) -> Pipe:
        if isinstance(df, str):
            return self.env.get_pipe(df)
        return make_pipe(df)

    def _set_declared_inputs(self, inputs: Union[NodeLike, Dict[str, NodeLike]]):
        self._declared_inputs = self.dfi.assign_inputs(inputs)

    def set_compiled_inputs(self, inputs: Dict[str, NodeLike]):
        self._compiled_inputs = inputs_as_nodes(self.env, inputs)
        if self.is_composite():
            self.get_input_node().set_compiled_inputs(inputs)

    def get_state(self, sess: Session) -> Optional[Dict]:
        state = sess.query(NodeState).filter(NodeState.node_name == self.name).first()
        if state:
            return state.state
        return None

    # def clone(self, new_name: str, **kwargs) -> Node:
    #     args = dict(
    #         env=self.env,
    #         name=new_name,
    #         pipe=self.pipe,
    #         config=self.config,
    #         inputs=self.get_declared_inputs(),
    #     )
    #     args.update(kwargs)
    #     return Node(**args)

    def get_dataset_node_name(self) -> str:
        return f"{self.name}__dataset"

    def get_dataset_name(self) -> str:
        return self._dataset_name or self.name

    def get_or_create_dataset_node(self, node_name: str = None) -> Node:
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
        dsn = self.env.add_node(
            name=node_name or self.get_dataset_node_name(),
            pipe=df,
            config={"dataset_name": self.get_dataset_name()},
            inputs=self,
        )
        logger.debug(f"Adding DataSet node {dsn}")
        return dsn

    def _get_interface(self) -> Optional[PipeInterface]:
        return self.pipe.get_interface(self.env)

    def get_interface(self) -> PipeInterface:
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
        return self.pipe.is_composite

    def as_stream(self) -> DataBlockStream:
        from dags.core.streams import DataBlockStream

        return DataBlockStream(upstream=self)

    def get_latest_output(self, ctx: ExecutionContext) -> Optional[DataBlock]:
        block = (
            ctx.metadata_session.query(DataBlockMetadata)
            .join(DataBlockLog)
            .join(PipeLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                PipeLog.node_name == self.name,
            )
            .order_by(DataBlockLog.created_at.desc())
            .first()
        )
        if block is None:
            return None
        return block.as_managed_data_block(ctx)


def build_composite_nodes(n: Node) -> Iterable[Node]:
    if not n.pipe.is_composite:
        raise
    nodes = []
    # TODO: just supports list for now
    raw_inputs = list(n.get_declared_inputs().values())
    assert len(raw_inputs) == 1, "Composite pipes take one input"
    input_node = raw_inputs[0]
    for fn in n.pipe.sub_graph:
        fn = ensure_pipe(n.env, fn)
        child_fn_key = make_pipe_key(fn)
        child_node_key = f"{n.name}__{child_fn_key}"
        try:
            node = n.env.get_node(child_node_key)
        except KeyError:
            node = Node(
                n.env,
                child_node_key,
                fn,
                config=n.config,
                inputs=input_node,
                declared_composite_node_name=n.declared_composite_node_name
                or n.name,  # Handle nested composite pipes
            )
        nodes.append(node)
        input_node = node
    return nodes

    # @property
    # def key(self):
    #     return self.name  # TODO


class NodeState(BaseModel):
    node_name = Column(String, primary_key=True)
    state = Column(JSON, nullable=True)

    def __repr__(self):
        return self._repr(node_name=self.node_name, state=self.state,)


def get_state(sess: Session, node_name: str) -> Optional[Dict]:
    state = sess.query(NodeState).filter(NodeState.node_name == node_name).first()
    if state:
        return state.state
    return None


class PipeLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_name = Column(String, nullable=False)
    node_start_state = Column(JSON, nullable=True)
    node_end_state = Column(JSON, nullable=True)
    pipe_key = Column(String, nullable=False)
    pipe_config = Column(JSON, nullable=True)
    runtime_url = Column(String, nullable=False)
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error = Column(JSON, nullable=True)
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="pipe_log"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            node_name=self.node_name,
            pipe_key=self.pipe_key,
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

    def persist_state(self, sess: Session) -> NodeState:
        state = (
            sess.query(NodeState).filter(NodeState.node_name == self.node_name).first()
        )
        if state is None:
            state = NodeState(node_name=self.node_name)
        state.state = self.node_end_state
        return sess.merge(state)


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
    pipe_log_id = Column(Integer, ForeignKey(PipeLog.id), nullable=False)
    data_block_id = Column(
        String, ForeignKey("dags_data_block_metadata.id"), nullable=False
    )  # TODO table name ref ugly here. We can parameterize with orm constant at least, or tablename("DataBlock.id")
    direction = Column(Enum(Direction, native_enum=False), nullable=False)
    processed_at = Column(DateTime, default=func.now(), nullable=False)
    # Hints
    data_block: "DataBlockMetadata"
    pipe_log: PipeLog

    def __repr__(self):
        return self._repr(
            id=self.id,
            pipe_log=self.pipe_log,
            data_block=self.data_block,
            direction=self.direction,
            processed_at=self.processed_at,
        )
