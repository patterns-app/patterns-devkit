from __future__ import annotations

import enum
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import JSON, DateTime, Enum, Integer, String

from dags.core.data_block import DataBlock, DataBlockMetadata
from dags.core.environment import Environment
from dags.core.metadata.orm import DAGS_METADATA_TABLE_PREFIX, BaseModel
from dags.core.pipe import Pipe, PipeLike, ensure_pipe, make_pipe, make_pipe_name
from dags.core.pipe_interface import (
    DeclaredNodeInput,
    DeclaredNodeLikeInput,
    PipeInterface,
)
from dags.utils.common import as_identifier
from loguru import logger

if TYPE_CHECKING:
    from dags.core.runnable import ExecutionContext
    from dags.core.streams import DataBlockStreamBuilder
    from dags.core.graph import Graph, GraphMetadata


NodeLike = Union[str, "Node"]


def inputs_as_nodes(
    graph: Graph, inputs: Dict[str, DeclaredNodeLikeInput]
) -> Dict[str, DeclaredNodeInput]:
    return {
        name: DeclaredNodeInput(
            node=graph.get_node(dnl.node_like),
            declared_schema_mapping=dnl.declared_schema_mapping,
        )
        for name, dnl in inputs.items()
    }


def create_node(
    graph: Graph,
    key: str,
    pipe: Union[PipeLike, str],
    upstream: Optional[Union[NodeLike, Dict[str, NodeLike]]] = None,  # Synonym
    config: Optional[Dict[str, Any]] = None,
    schema_mapping: Optional[Dict[str, Union[Dict[str, str], str]]] = None,
    output_alias: Optional[str] = None,
    create_dataset: bool = True,
    dataset_name: Optional[str] = None,
):
    config = config or {}
    env = graph.env
    if isinstance(pipe, str):
        pipe = env.get_pipe(pipe)
    else:
        pipe = make_pipe(pipe)
    interface = pipe.get_interface(env)
    schema_mapping = interface.assign_mapping(schema_mapping)
    _declared_inputs: Dict[str, DeclaredNodeLikeInput] = {}
    n = Node(
        env=graph.env,
        graph=graph,
        key=key,
        pipe=pipe,
        config=config,
        interface=interface,
        _declared_inputs=_declared_inputs,
        _declared_schema_mapping=schema_mapping,
        output_alias=output_alias,
        create_dataset=create_dataset,
        _dataset_name=dataset_name,
    )
    if upstream is not None:
        for name, node_like in interface.assign_inputs(upstream).items():
            n._declared_inputs[name] = DeclaredNodeLikeInput(
                node_like=node_like,
                declared_schema_mapping=(schema_mapping or {}).get(name),
            )
    return n


@dataclass(frozen=True)
class Node:
    env: Environment
    graph: Graph
    key: str
    pipe: Pipe
    config: Dict[str, Any]
    interface: PipeInterface
    _declared_inputs: Dict[str, DeclaredNodeLikeInput]
    create_dataset: bool = True
    output_alias: Optional[str] = None
    _dataset_name: Optional[str] = None
    _declared_schema_mapping: Optional[Dict[str, Dict[str, str]]] = None

    def __repr__(self):
        return f"<{self.__class__.__name__}(key={self.key}, pipe={self.pipe.key})>"

    def __hash__(self):
        return hash(self.key)

    def get_state(self, sess: Session) -> Optional[Dict]:
        state = sess.query(NodeState).filter(NodeState.node_key == self.key).first()
        if state:
            return state.state
        return None

    def get_dataset_name(self) -> str:
        return self._dataset_name or self.key

    def get_alias(self) -> str:
        if self.output_alias:
            ident = self.output_alias
        else:
            ident = f"_{self.key}__latest"
        return as_identifier(
            ident
        )  # TODO: this logic should be storage api specific! and then shared back?

    def get_dataset_node_keys(self):
        return [
            f"{self.key}__accumulator",
            f"{self.key}__dedupe",
        ]

    def create_dataset_nodes(self) -> List[Node]:
        dfi = self.get_interface()
        if dfi.output is None:
            raise
        # if self.output_is_dataset():
        #     return []
        # TODO: how do we choose runtime? just using lang for now
        lang = self.pipe.source_code_language()
        if lang == "sql":
            df_accum = "core.sql_accumulator"
            df_dedupe = "core.sql_dedupe_unique_keep_newest_row"
        else:
            df_accum = "core.dataframe_accumulator"
            df_dedupe = "core.dataframe_dedupe_unique_keep_newest_row"
        accum_key, dedupe_key = self.get_dataset_node_keys()
        accum = create_node(
            self.graph,
            key=accum_key,
            pipe=self.env.get_pipe(df_accum),
            upstream=self,
        )
        dedupe = create_node(
            self.graph,
            key=dedupe_key,
            pipe=self.env.get_pipe(df_dedupe),
            upstream=accum,
            output_alias=self.get_dataset_name(),
        )
        logger.debug(f"Adding DataSet nodes {[accum, dedupe]}")
        return [accum, dedupe]

    def get_interface(self) -> PipeInterface:
        return self.interface

    def get_declared_input_nodes(self) -> Dict[str, DeclaredNodeInput]:
        return inputs_as_nodes(self.graph, self.get_declared_inputs())

    def get_declared_inputs(self) -> Dict[str, DeclaredNodeLikeInput]:
        return self._declared_inputs or {}

    def get_schema_mapping_for_input(self, input_name: str) -> Optional[Dict[str, str]]:
        return (self._declared_schema_mapping or {}).get(input_name)

    def as_stream(self) -> DataBlockStreamBuilder:
        from dags.core.streams import DataBlockStreamBuilder

        return DataBlockStreamBuilder(upstream=self)

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
    node_key = Column(String, primary_key=True)
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
        String,
        ForeignKey(f"{DAGS_METADATA_TABLE_PREFIX}graph_metadata.hash"),
        nullable=False,
    )
    node_key = Column(String, nullable=False)
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
        String,
        ForeignKey(f"{DAGS_METADATA_TABLE_PREFIX}data_block_metadata.id"),
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
