from __future__ import annotations

import enum
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.environment import Environment
from snapflow.core.metadata.orm import (
    SNAPFLOW_METADATA_TABLE_PREFIX,
    BaseModel,
)
from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.expression import select, update
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey, UniqueConstraint
from sqlalchemy.sql.sqltypes import JSON, Boolean, DateTime, Enum, Integer, String


class NodeState(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_key = Column(String(128))
    state = Column(JSON, nullable=True)

    __table_args__ = (UniqueConstraint("env_id", "node_key"),)

    def __repr__(self):
        return self._repr(node_key=self.node_key, state=self.state,)


def get_state(env: Environment, node_key: str) -> Optional[NodeState]:
    return env.md_api.execute(
        select(NodeState).filter(NodeState.node_key == node_key)
    ).scalar_one_or_none()


class DataFunctionLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_key = Column(String(128), nullable=False)
    node_start_state = Column(JSON, nullable=True)
    node_end_state = Column(JSON, nullable=True)
    function_key = Column(String(128), nullable=False)
    function_params = Column(JSON, nullable=True)
    runtime_url = Column(String(128), nullable=True)  # TODO
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error = Column(JSON, nullable=True)
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="function_log"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            graph_id=self.graph_id,
            node_key=self.node_key,
            function_key=self.function_key,
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
        self.error = {"error": str(e) or type(e).__name__, "traceback": tback[:5000]}

    def persist_state(self, env: Environment) -> NodeState:
        state = env.md_api.execute(
            select(NodeState).filter(NodeState.node_key == self.node_key)
        ).scalar_one_or_none()
        if state is None:
            state = NodeState(node_key=self.node_key)
            env.md_api.add(state)
        state.state = self.node_end_state
        env.md_api.flush([state])
        return state


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
    function_log_id = Column(Integer, ForeignKey(DataFunctionLog.id), nullable=False)
    data_block_id = Column(
        String(128),
        ForeignKey(f"{SNAPFLOW_METADATA_TABLE_PREFIX}data_block_metadata.id"),
        nullable=False,
    )
    stream_name = Column(String(128), nullable=True)
    direction = Column(Enum(Direction, native_enum=False), nullable=False)
    processed_at = Column(DateTime, default=func.now(), nullable=False)
    invalidated = Column(Boolean, default=False)
    # Hints
    data_block: "DataBlockMetadata"
    function_log: DataFunctionLog

    def __repr__(self):
        return self._repr(
            id=self.id,
            function_log=self.function_log,
            data_block=self.data_block,
            direction=self.direction,
            processed_at=self.processed_at,
        )

    @classmethod
    def summary(cls, env: Environment) -> str:
        s = ""
        for dbl in env.md_api.execute(select(DataBlockLog)).scalars().all():
            s += f"{dbl.function_log.node_key:50}"
            s += f"{str(dbl.data_block_id):23}"
            s += f"{str(dbl.data_block.record_count):6}"
            s += f"{dbl.direction.value:9}{str(dbl.data_block.updated_at):22}"
            s += f"{dbl.data_block.nominal_schema_key:20}{dbl.data_block.realized_schema_key:20}\n"
        return s

