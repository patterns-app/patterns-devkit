from __future__ import annotations

from enum import Enum
from snapflow.core.function_interface_manager import bind_inputs
from snapflow.core.environment import Environment
import traceback

from commonmodel.base import Schema
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
)

from dcp.storage.base import Storage
from snapflow.core.declarative.base import FrozenPydanticBase, PydanticBase
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.graph import GraphCfg, NodeInputCfg
from typing import (
    Dict,
    Set,
    TYPE_CHECKING,
    List,
    Optional,
    Union,
)

if TYPE_CHECKING:
    from snapflow.core.streams import DataBlockStream, StreamBuilder


class ExecutionCfg(FrozenPydanticBase):
    dataspace: DataspaceCfg
    target_storage: str
    local_storage: Optional[str] = None
    target_data_format: Optional[str] = None
    storages: List[str] = []
    run_until_inputs_exhausted: bool = True
    # TODO: this is a "soft" limit, could imagine a "hard" one too
    execution_timelimit_seconds: Optional[int] = None

    def get_target_storage(self) -> Storage:
        return Storage(self.target_storage)

    def get_local_storage(self) -> Optional[Storage]:
        if self.local_storage is None:
            return None
        return Storage(self.local_storage)

    def get_storages(self) -> List[Storage]:
        return [Storage(s) for s in self.storages]


class ExecutableCfg(FrozenPydanticBase):
    node_key: str
    graph: GraphCfg
    execution_config: ExecutionCfg

    @property
    def node(self) -> GraphCfg:
        return self.graph.get_node(self.node_key)

    def get_bound_interface(self, env: Environment) -> BoundInterfaceCfg:
        node_inputs = self.node.get_node_inputs(self.graph)
        bound_inputs = bind_inputs(env, self, node_inputs)
        return BoundInterfaceCfg(
            inputs=bound_inputs, interface=self.node.get_interface(),
        )


class BoundInterfaceCfg(FrozenPydanticBase):
    inputs: Dict[str, NodeInputCfg]
    interface: DataFunctionInterfaceCfg

    def inputs_as_kwargs(self) -> Dict[str, Union[DataBlock, DataBlockStream]]:
        assert all([i.is_bound() for i in self.inputs.values()])
        return {
            i.name: i.bound_stream if i.is_stream else i.bound_block
            for i in self.inputs.values()
        }

    def non_reference_bound_inputs(self) -> List[NodeInputCfg]:
        return [
            i
            for i in self.inputs.values()
            if i.bound_stream is not None and not i.input.is_reference
        ]

    def resolve_nominal_output_schema(self) -> Optional[str]:
        output = self.interface.get_default_output()
        if not output:
            return None
        if not output.is_generic:
            return output.schema_key
        output_generic = output.schema_key
        for node_input in self.inputs.values():
            if not node_input.input.is_generic:
                continue
            if node_input.input.schema_key == output_generic:
                schema = node_input.get_bound_nominal_schema()
                # We check if None -- there may be more than one input with same generic, we'll take any that are resolvable
                if schema is not None:
                    return schema.key
        raise Exception(f"Unable to resolve generic '{output_generic}'")


class ExecutionResult(PydanticBase):
    inputs_bound: List[str]
    non_reference_inputs_bound: List[str]
    input_block_counts: Dict[str, int]
    output_blocks: Optional[Dict[str, Dict]] = None
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


class CumulativeExecutionResult(PydanticBase):
    input_block_counts: Dict[str, int] = {}
    output_blocks: Optional[Dict[str, List[Dict]]] = {}
    error: Optional[str] = None
    traceback: Optional[str] = None

    def add_result(self, result: ExecutionResult):
        for i, c in result.input_block_counts.items():
            self.input_block_counts[i] = self.input_block_counts.setdefault(i, 0) + c
        for i, dbs in result.output_blocks.items():
            self.output_blocks.setdefault(i, []).append(dbs)
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

