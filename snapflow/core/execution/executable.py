from __future__ import annotations
from contextlib import contextmanager
from datacopy.data_format.base import DataFormat

from datacopy.storage.base import Storage, ensure_storage
from snapflow.core.snap import _Snap
from snapflow.core.node import DeclaredNode, Node, NodeConfiguration
from snapflow.core.graph import DeclaredGraph, NxAdjacencyList, graph_from_node_configs

import traceback
from dataclasses import dataclass
from typing import (
    Callable,
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
)
from snapflow.core.environment import Environment, EnvironmentConfiguration
from snapflow.core.snap_interface import StreamInput


class ExecutionLogger:
    def __init__(self, out: Callable = lambda x: print(x, end="")):
        self.out = out
        self.curr_indent = 0

    @contextmanager
    def indent(self, n=4):
        self.curr_indent += n
        yield
        self.curr_indent = max(self.curr_indent - n, 0)

    def log(self, msg: str, prefix="", suffix="", indent: int = 0):
        total_indent = self.curr_indent + indent
        for ln in msg.split("\n"):
            if not ln:
                continue
            self.out(total_indent * " " + prefix + ln + suffix + "\n")


@dataclass(frozen=True)
class ExecutionConfiguration:
    env_config: EnvironmentConfiguration
    local_storage_url: str
    target_storage_url: str
    target_format: str = None
    storage_urls: List[str] = None
    # runtime_urls: List[str] = None
    # run_until_inputs_exhausted: bool = True  # TODO: punt on repeated runs for now, v confusing
    # TODO: this is a "soft" limit, could imagine a "hard" one too
    execution_timelimit_seconds: Optional[int] = None
    logger: str = None  # TODO: how to specify this in serializable way?


@dataclass(frozen=True)
class ExecutionContext:
    env: Environment
    local_storage: Storage
    target_storage: Storage
    target_format: Optional[DataFormat] = None
    storages: List[Storage] = None
    logger: ExecutionLogger = None
    execution_timelimit_seconds: Optional[int] = None
    raise_on_snap_error: bool = False
    execution_config: ExecutionConfiguration = None

    @staticmethod
    def from_config(cfg: ExecutionConfiguration) -> ExecutionContext:
        env = Environment.from_config(cfg.env_config)
        return ExecutionContext(
            env=env,
            local_storage=ensure_storage(cfg.local_storage_url),
            target_storage=ensure_storage(cfg.target_storage_url),
            target_format=None,  # TODO: from config
            storages=[ensure_storage(s) for s in cfg.storage_urls],
            logger=ExecutionLogger(),  # TODO: from config
            execution_timelimit_seconds=cfg.execution_timelimit_seconds,
            raise_on_snap_error=env.settings.raise_on_snap_error,
            execution_config=cfg,
        )


@dataclass
class ExecutableConfiguration:
    node_key: str
    snap_key: str
    execution_config: ExecutionConfiguration
    nodes: Dict[str, NodeConfiguration]


@dataclass(frozen=True)
class Executable:
    node: Node
    snap: _Snap
    execution_context: ExecutionContext
    executable_config: ExecutableConfiguration = None
    # graph_adjacency: NxAdjacencyList # Graph is implied by node_key (nodes only belong to one graph)

    @staticmethod
    def from_config(cfg: ExecutableConfiguration) -> Executable:
        ec = ExecutionContext.from_config(cfg.execution_config)
        graph = graph_from_node_configs(ec.env, cfg.nodes.values())
        return Executable(
            node=graph.get_node(cfg.node_key),
            snap=ec.env.get_snap(cfg.snap_key),
            execution_context=ec,
            executable_config=cfg,
        )


# @dataclass
# class DataBlockSummary:
#     id: str
#     record_count: Optional[int] = None
#     alias: Optional[str] = None


@dataclass
class ExecutionResult:
    inputs_bound: List[str]
    non_reference_inputs_bound: List[StreamInput]
    input_block_counts: Dict[str, int]
    output_blocks: Optional[Dict[str, Dict]] = None
    # output_block_record_counts: Optional[Dict[str, int]] = None
    # output_block_ids: Optional[Dict[str, str]] = None
    # output_block_aliases: Optional[Dict[str, str]] = None
    error: Optional[str] = None
    traceback: Optional[str] = None

    @classmethod
    def empty(cls) -> ExecutionResult:
        return ExecutionResult(
            inputs_bound=[], non_reference_inputs_bound=[], input_block_counts={},
        )

    def set_error(self, e: Exception):
        tback = traceback.format_exc()
        self.error = (
            str(e) or type(e).__name__
        )  # MUST evaluate true if there's an error!
        # Traceback can be v large (like in max recursion), so we truncate to 5k chars
        self.traceback = tback[:5000]

