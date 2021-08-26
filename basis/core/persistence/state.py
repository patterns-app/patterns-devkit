from __future__ import annotations

import enum
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from basis.core.declarative.base import PydanticBase
from basis.core.environment import Environment
from basis.core.persistence.base import BASIS_METADATA_TABLE_PREFIX, BaseModel
from basis.core.persistence.block import BlockMetadata
from pydantic_sqlalchemy.main import sqlalchemy_to_pydantic
from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.expression import select, update
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey, UniqueConstraint
from sqlalchemy.sql.sqltypes import JSON, Boolean, DateTime, Enum, Integer, String


if TYPE_CHECKING:
    from basis.core.persistence.pydantic import BlockMetadataCfg


class ExecutionLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_key = Column(String(128), nullable=False)
    node_cfg = Column(JSON, nullable=True)
    runtime_url = Column(String(128), nullable=True)  # TODO
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    timed_out = Column(Boolean, default=False, nullable=True)
    error = Column(JSON, nullable=True)
    # block_logs: RelationshipProperty = relationship("BlockLog", backref="function_log")

    def __repr__(self):
        return self._repr(
            id=self.id,
            node_key=self.node_key,
            function_key=self.function_key,
            started_at=self.started_at,
            completed_at=self.completed_at,
        )

    @classmethod
    def from_pydantic(cls, cfg: PydanticBase) -> ExecutionLog:
        return ExecutionLog(**cfg.dict())

    def output_blocks(self) -> Iterable[BlockMetadata]:
        return [dbl for dbl in self.block_logs if dbl.direction == Direction.OUTPUT]

    def input_blocks(self) -> Iterable[BlockMetadata]:
        return [dbl for dbl in self.block_logs if dbl.direction == Direction.INPUT]

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


class Direction(str, enum.Enum):
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


# class BlockLog(BaseModel):
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     function_log_id = Column(Integer, ForeignKey(ExecutionLog.id), nullable=False)
#     block_id = Column(
#         String(128),
#         ForeignKey(f"{BASIS_METADATA_TABLE_PREFIX}block_metadata.id"),
#         nullable=False,
#     )
#     stream_name = Column(String(128), nullable=True)
#     direction = Column(Enum(Direction, native_enum=False), nullable=False)
#     processed_at = Column(DateTime, default=func.now(), nullable=False)
#     invalidated = Column(Boolean, default=False)
#     # Hints
#     block: "BlockMetadata"
#     function_log: ExecutionLog

#     def __repr__(self):
#         return self._repr(
#             id=self.id,
#             function_log=self.function_log,
#             block=self.block,
#             direction=self.direction,
#             processed_at=self.processed_at,
#         )

#     @classmethod
#     def summary(cls, env: Environment) -> str:
#         s = ""
#         for dbl in env.md_api.execute(select(BlockLog)).scalars().all():
#             s += f"{dbl.function_log.node_key:50}"
#             s += f"{str(dbl.block_id):23}"
#             s += f"{str(dbl.block.record_count):6}"
#             s += f"{dbl.direction.value:9}{str(dbl.block.updated_at):22}"
#             s += (
#                 f"{dbl.block.nominal_schema_key:20}{dbl.block.realized_schema_key:20}\n"
#             )
#         return s


# class NodeState(BaseModel):
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     node_key = Column(String(128))
#     state = Column(JSON, nullable=True)
#     latest_log_id = Column(Integer, ForeignKey(ExecutionLog.id), nullable=True)
#     blocks_output_stdout = Column(Integer, nullable=True)
#     blocks_output_all = Column(Integer, nullable=True)
#     latest_log: RelationshipProperty = relationship("ExecutionLog")

#     __table_args__ = (UniqueConstraint("env_id", "node_key"),)

#     def __repr__(self):
#         return self._repr(
#             node_key=self.node_key, state=self.state, latest_log_id=self.latest_log_id,
#         )


class StreamState(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_key = Column(String(128))
    stream_name = Column(String(128), nullable=False)
    direction = Column(Enum(Direction, native_enum=False), nullable=False)
    first_processed_block_id = Column(String(128), nullable=True)
    latest_processed_block_id = Column(String(128), nullable=True)
    latest_processed_at = Column(DateTime, nullable=True)
    blocks_processed = Column(Integer, default=0)

    def __repr__(self):
        return self._repr(
            node_key=self.node_key,
            start_block_id=self.start_block_id,
            end_block_id=self.end_block_id,
            block_count=self.block_count,
        )

    def update_with_blocks(self, blocks: List[BlockMetadataCfg]):
        min_block_id = min([b.id for b in blocks])
        max_block_id = max([b.id for b in blocks])
        if self.first_processed_block_id is None:
            self.first_processed_block_id = min_block_id
        else:
            self.first_processed_block_id = min(
                min_block_id, self.first_processed_block_id
            )
        if self.latest_processed_block_id is None:
            self.latest_processed_block_id = max_block_id
        else:
            self.latest_processed_block_id = max(
                max_block_id, self.latest_processed_block_id
            )
        self.blocks_processed += len(blocks)


def get_state(
    env: Environment,
    node_key: str,
    stream_name: str = None,
    direction: Direction = None,
) -> Optional[StreamState]:
    q = select(StreamState).filter(StreamState.node_key == node_key)
    if stream_name:
        q = q.filter(StreamState.stream_name == stream_name)
    if direction:
        q = q.filter(StreamState.direction == direction)
    state = env.md_api.execute(q).scalar_one_or_none()
    return state


def get_or_create_state(
    env: Environment,
    node_key: str,
    stream_name: str = None,
    direction: Direction = None,
) -> StreamState:
    state = get_state(env, node_key, stream_name, direction)
    if state is None:
        state = StreamState(
            node_key=node_key, stream_name=stream_name, direction=direction
        )
    return state


def _reset_state(env: Environment, node_key: str):
    """
    Resets the node's state only.
    This is usually not something you want to do by itself, but
    instead as part of a full reset.
    """
    state = get_state(env, node_key)
    if state is not None:
        env.md_api.delete(state)


# def _invalidate_blocks(env: Environment, node_key: str):
#     """"""
#     dbl_ids = [
#         r
#         for r in env.md_api.execute(
#             select(BlockLog.id)
#             .join(ExecutionLog)
#             .filter(ExecutionLog.node_key == node_key)
#         )
#         .scalars()
#         .all()
#     ]
#     env.md_api.execute(
#         update(BlockLog)
#         .filter(BlockLog.id.in_(dbl_ids))
#         .values({"invalidated": True})
#         .execution_options(synchronize_session="fetch")  # TODO: or false?
#     )


def reset(env: Environment, node_key: str):
    """
    Resets the node, meaning all state is cleared, and all OUTPUT and INPUT block
    logs are invalidated. Output blocks are NOT deleted.
    NB: If downstream nodes have already processed an output block,
    this will have no effect on them.
    TODO: consider "cascading reset" for downstream recursive functions (which will still have
    accumulated output from this node)
    TODO: especially augmentation nodes! (accum, dedupe)
    """
    _reset_state(env, node_key)
    # _invalidate_blocks(env, node_key)
