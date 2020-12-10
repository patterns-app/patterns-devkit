from __future__ import annotations

import enum
import traceback
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from loguru import logger
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.environment import Environment
from snapflow.core.metadata.orm import SNAPFLOW_METADATA_TABLE_PREFIX, BaseModel
from snapflow.core.pipe import Pipe, PipeLike, ensure_pipe, make_pipe, make_pipe_name
from snapflow.core.pipe_interface import (
    DeclaredStreamInput,
    DeclaredStreamLikeInput,
    PipeInterface,
)
from snapflow.utils.common import as_identifier
from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import JSON, DateTime, Enum, Integer, String

if TYPE_CHECKING:
    from snapflow.core.runnable import ExecutionContext
    from snapflow.core.streams import StreamBuilder, StreamLike
    from snapflow.core.graph import Graph, GraphMetadata, DeclaredGraph, DEFAULT_GRAPH

NodeLike = Union[str, "Node", "DeclaredNode"]
NodeBase = Union["Node", "DeclaredNode"]


def ensure_stream(stream_like: StreamLike) -> StreamBuilder:
    from snapflow.core.streams import StreamBuilder, StreamLike

    if isinstance(stream_like, StreamBuilder):
        return stream_like
    if isinstance(stream_like, DeclaredNode) or isinstance(stream_like, Node):
        return stream_like.as_stream_builder()
    if isinstance(stream_like, str):
        return StreamBuilder(nodes=[stream_like])
    raise TypeError(stream_like)


@dataclass
class DeclaredNode:
    pipe: Union[PipeLike, str]
    key: str
    config: Dict[str, Any] = field(default_factory=dict)
    upstream: Union[StreamLike, Dict[str, StreamLike]] = field(default_factory=dict)
    graph: Optional[DeclaredGraph] = None
    output_alias: Optional[str] = None
    schema_translation: Optional[Dict[str, Union[Dict[str, str], str]]] = None

    def __post_init__(self):
        from snapflow.core.graph import DEFAULT_GRAPH

        # Ensure node is in graph (or put in default graph)
        # TODO: better way to do this?
        if self.graph is None:
            self.graph = DEFAULT_GRAPH
        # self.graph.add_node(self)

    def __repr__(self):
        return f"<{self.__class__.__name__}(key={self.key}, pipe={self.pipe})>"

    def __hash__(self):
        return hash(self.key)

    def set_upstream(self, *args, **kwargs):
        """If a single positional argument, overwrites upstream.
        If kwargs then updates any existing upstream.
        """
        if args:
            if len(args) > 1 or kwargs:
                raise Exception("Provide keyword args for multiple inputs to upstream")
            self.upstream = args[0]
        else:
            if isinstance(self.upstream, dict):
                self.upstream.update(kwargs)
            else:
                self.upstream = kwargs

    def as_stream_builder(self) -> StreamBuilder:
        from snapflow.core.streams import StreamBuilder

        return StreamBuilder(nodes=self)

    def instantiate(self, env: Environment, g: Graph = None) -> Node:
        from snapflow.core.graph import Graph

        if g is None:
            g = Graph(env)
        return instantiate_node(env, g, self)


def node(
    pipe: Union[PipeLike, str],
    key: Optional[str] = None,
    config: Dict[str, Any] = None,
    upstream: Union[StreamLike, Dict[str, StreamLike]] = None,
    graph: Optional[DeclaredGraph] = None,
    output_alias: Optional[str] = None,
    schema_translation: Optional[Dict[str, Union[Dict[str, str], str]]] = None,
) -> DeclaredNode:
    if key is None:
        key = make_pipe_name(pipe)
    return DeclaredNode(
        pipe=pipe,
        key=key,
        config=config or {},
        upstream=upstream or {},
        graph=graph,
        output_alias=output_alias,
        schema_translation=schema_translation,
    )


def instantiate_node(
    env: Environment,
    graph: Graph,
    declared_node: DeclaredNode,
):
    if isinstance(declared_node.pipe, str):
        pipe = env.get_pipe(declared_node.pipe)
    else:
        pipe = make_pipe(declared_node.pipe)
    interface = pipe.get_interface()
    schema_translation = interface.assign_translations(declared_node.schema_translation)
    declared_inputs: Dict[str, DeclaredStreamInput] = {}
    if declared_node.upstream is not None:
        for name, stream_like in interface.assign_inputs(
            declared_node.upstream
        ).items():
            declared_inputs[name] = DeclaredStreamInput(
                stream=ensure_stream(stream_like),
                declared_schema_translation=(schema_translation or {}).get(name),
            )
    n = Node(
        env=env,
        graph=graph,
        key=declared_node.key,
        pipe=pipe,
        config=declared_node.config,
        interface=interface,
        declared_inputs=declared_inputs,
        declared_schema_translation=schema_translation,
        output_alias=declared_node.output_alias,
    )
    return n


def make_default_output_alias(node: Node) -> str:
    return as_identifier(
        f"_{node.key}__latest"
    )  # TODO: as_identifier is storage-specific


@dataclass(frozen=True)
class Node:
    env: Environment
    graph: Graph
    key: str
    pipe: Pipe
    config: Dict[str, Any]
    interface: PipeInterface
    declared_inputs: Dict[str, DeclaredStreamInput]
    output_alias: Optional[str] = None
    declared_schema_translation: Optional[Dict[str, Dict[str, str]]] = None

    def __repr__(self):
        return f"<{self.__class__.__name__}(key={self.key}, pipe={self.pipe.key})>"

    def __hash__(self):
        return hash(self.key)

    def get_state(self, sess: Session) -> Optional[Dict]:
        state = sess.query(NodeState).filter(NodeState.node_key == self.key).first()
        if state:
            return state.state
        return None

    def get_alias(self) -> str:
        if self.output_alias:
            ident = self.output_alias
        else:
            ident = make_default_output_alias(self)
        return as_identifier(
            ident
        )  # TODO: this logic should be storage api specific! and then shared back?

    def get_interface(self) -> PipeInterface:
        return self.interface

    def get_schema_translation_for_input(
        self, input_name: str
    ) -> Optional[Dict[str, str]]:
        return (self.declared_schema_translation or {}).get(input_name)

    def as_stream_builder(self) -> StreamBuilder:
        from snapflow.core.streams import StreamBuilder

        return StreamBuilder(nodes=self)

    def get_latest_output(self, ctx: ExecutionContext) -> Optional[DataBlock]:
        block = (
            ctx.metadata_session.query(DataBlockMetadata)
            .join(DataBlockLog)
            .join(PipeLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                PipeLog.node_key == self.key,
            )
            .order_by(DataBlockLog.created_at.desc())
            .first()
        )
        if block is None:
            return None
        return block.as_managed_data_block(ctx)


class NodeState(BaseModel):
    node_key = Column(String(128), primary_key=True)
    state = Column(JSON, nullable=True)

    def __repr__(self):
        return self._repr(
            node_key=self.node_key,
            state=self.state,
        )


def get_state(sess: Session, node_key: str) -> Optional[Dict]:
    state = sess.query(NodeState).filter(NodeState.node_key == node_key).first()
    if state:
        return state.state
    return None


class PipeLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    graph_id = Column(
        String(128),
        ForeignKey(f"{SNAPFLOW_METADATA_TABLE_PREFIX}graph_metadata.hash"),
        nullable=False,
    )
    node_key = Column(String(128), nullable=False)
    node_start_state = Column(JSON, nullable=True)
    node_end_state = Column(JSON, nullable=True)
    pipe_key = Column(String(128), nullable=False)
    pipe_config = Column(JSON, nullable=True)
    runtime_url = Column(String(128), nullable=False)
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error = Column(JSON, nullable=True)
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="pipe_log"
    )
    graph: "GraphMetadata"

    def __repr__(self):
        return self._repr(
            id=self.id,
            graph_id=self.graph_id,
            node_key=self.node_key,
            pipe_key=self.pipe_key,
            runtime_url=self.runtime_url,
            started_at=self.started_at,
        )

    def output_data_blocks(self) -> Iterable[DataBlockMetadata]:
        return [
            dbl for dbl in self.data_block_logs if dbl.direction == Direction.OUTPUT
        ]

    def input_data_blocks(self) -> Iterable[DataBlockMetadata]:
        return [dbl for dbl in self.data_block_logs if dbl.direction == Direction.INPUT]

    def set_error(self, e: Exception):
        tback = traceback.format_exc()
        # Traceback can be v large (like in max recursion), so we truncate to 5k chars
        self.error = {"error": str(e), "traceback": tback[:5000]}

    def persist_state(self, sess: Session) -> NodeState:
        state = (
            sess.query(NodeState).filter(NodeState.node_key == self.node_key).first()
        )
        if state is None:
            state = NodeState(node_key=self.node_key)
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
        String(128),
        ForeignKey(f"{SNAPFLOW_METADATA_TABLE_PREFIX}data_block_metadata.id"),
        nullable=False,
    )
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

    @classmethod
    def summary(cls, env: Environment) -> str:
        s = ""
        for dbl in env.session.query(DataBlockLog).all():
            s += f"{dbl.pipe_log.node_key:50}"
            s += f"{str(dbl.data_block_id):23}"
            s += f"{str(dbl.data_block.record_count):6}"
            s += f"{dbl.direction.value:9}{str(dbl.data_block.updated_at):22}"
            s += f"{dbl.data_block.nominal_schema_key:20}{dbl.data_block.realized_schema_key:20}\n"
        return s
