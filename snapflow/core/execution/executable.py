from __future__ import annotations

import traceback
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from dcp.data_format.base import DataFormat
from dcp.storage.base import Storage, ensure_storage
from loguru import logger
from snapflow.core.data_block import (
    DataBlock,
    DataBlockMetadata,
    ManagedDataBlock,
    StoredDataBlockMetadata,
)
from snapflow.core.environment import Environment, EnvironmentConfiguration
from snapflow.core.function import DEFAULT_OUTPUT_NAME, DataFunction
from snapflow.core.function_interface_manager import StreamInput
from snapflow.core.graph import DeclaredGraph, NxAdjacencyList, graph_from_node_configs
from snapflow.core.node import DataFunctionLog, DeclaredNode, Node, NodeConfiguration
from sqlalchemy.sql.expression import select


class ExecutionLogger:
    def __init__(self, out: Callable = lambda x: print(x, end="")):
        self.out = out
        self.curr_indent = 0
        self.indent_size = 4

    @contextmanager
    def indent(self, n=1):
        self.curr_indent += n * self.indent_size
        yield
        self.curr_indent = max(self.curr_indent - n * self.indent_size, 0)

    def log(self, msg: str, prefix="", suffix="", indent: int = 0):
        total_indent = self.curr_indent + indent * self.indent_size
        lines = msg.strip("\n").split("\n")
        full_prefix = total_indent * " " + prefix
        sep = suffix + "\n" + full_prefix
        message = full_prefix + sep.join(lines) + suffix
        self.out(message)
        if msg.endswith("\n"):
            self.out("\n")

    def log_token(self, msg: str):
        self.out(msg)


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
    logger: ExecutionLogger = field(default_factory=ExecutionLogger)
    execution_timelimit_seconds: Optional[int] = None
    abort_on_function_error: bool = False
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
            # logger=ExecutionLogger(),  # TODO: from config
            execution_timelimit_seconds=cfg.execution_timelimit_seconds,
            abort_on_function_error=env.settings.abort_on_function_error,
            execution_config=cfg,
        )


@dataclass
class ExecutableConfiguration:
    node_key: str
    function_key: str
    execution_config: ExecutionConfiguration
    nodes: Dict[str, NodeConfiguration]


@dataclass(frozen=True)
class Executable:
    node: Node
    function: DataFunction
    execution_context: ExecutionContext
    executable_config: ExecutableConfiguration = None
    # graph_adjacency: NxAdjacencyList # Graph is implied by node_key (nodes only belong to one graph)

    @staticmethod
    def from_config(cfg: ExecutableConfiguration) -> Executable:
        ec = ExecutionContext.from_config(cfg.execution_config)
        graph = graph_from_node_configs(ec.env, cfg.nodes.values())
        return Executable(
            node=graph.get_node(cfg.node_key),
            function=ec.env.get_function(cfg.function_key),
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
    error: Optional[str] = None
    traceback: Optional[str] = None

    @classmethod
    def empty(cls) -> ExecutionResult:
        return ExecutionResult(
            inputs_bound=[],
            non_reference_inputs_bound=[],
            input_block_counts={},
        )

    def set_error(self, e: Exception):
        tback = traceback.format_exc()
        self.error = (
            str(e) or type(e).__name__
        )  # MUST evaluate true if there's an error!
        # Traceback can be v large (like in max recursion), so we truncate to 5k chars
        self.traceback = tback[:5000]

    def get_output_block(
        self, env: Environment, name: Optional[str] = None
    ) -> Optional[DataBlock]:

        if not self.output_blocks:
            return None
        if name:
            dbid = self.output_blocks[name]["id"]
        else:
            dbid = self.output_blocks[DEFAULT_OUTPUT_NAME]["id"]
        env.md_api.begin()  # TODO: hanging session
        block = env.md_api.execute(
            select(DataBlockMetadata).filter(DataBlockMetadata.id == dbid)
        ).scalar_one()
        mds = block.as_managed_data_block(env)
        return mds


@dataclass
class CumulativeExecutionResult:
    input_block_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    output_blocks: Optional[Dict[str, List[Dict]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    error: Optional[str] = None
    traceback: Optional[str] = None

    def add_result(self, result: ExecutionResult):
        for i, c in result.input_block_counts.items():
            self.input_block_counts[i] += c
        for i, dbs in result.output_blocks.items():
            self.output_blocks[i].append(dbs)
        if result.error:
            self.error = result.error
            self.traceback = result.traceback

    def get_output_blocks(
        self, env: Environment, name: Optional[str] = None
    ) -> List[DataBlock]:
        blocks = []
        if not self.output_blocks:
            return blocks
        env.md_api.begin()  # TODO: hanging session
        for bs in self.output_blocks[name or DEFAULT_OUTPUT_NAME]:
            dbid = bs["id"]
            block = env.md_api.execute(
                select(DataBlockMetadata).filter(DataBlockMetadata.id == dbid)
            ).scalar_one()
            mds = block.as_managed_data_block(env)
            blocks.append(mds)
        return blocks
