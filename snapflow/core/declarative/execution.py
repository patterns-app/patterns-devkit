from __future__ import annotations
from snapflow.core.declarative.data_block import (
    DataBlockMetadataCfg,
    StoredDataBlockMetadataCfg,
)

import traceback
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from commonmodel.base import Schema
from dcp.storage.base import Storage
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.declarative.base import FrozenPydanticBase, PydanticBase
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
)
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment
from sqlalchemy.sql.expression import select

if TYPE_CHECKING:
    from snapflow.core.streams import DataBlockStream, StreamBuilder
    from snapflow.core.function_interface_manager import (
        BoundInput,
        BoundInterface,
    )


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

    def get_bound_interface(self, env: Environment) -> BoundInterface:
        from snapflow.core.function_interface_manager import BoundInterface, bind_inputs

        node_inputs = self.node.get_node_inputs(self.graph)
        bound_inputs = bind_inputs(env, self, node_inputs)
        return BoundInterface(inputs=bound_inputs, interface=self.node.get_interface(),)


class NodeInputCfg(FrozenPydanticBase):
    name: str
    input: DataFunctionInputCfg
    input_node: Optional[GraphCfg] = None
    schema_translation: Optional[Dict[str, str]] = None

    def as_stream_builder(self) -> StreamBuilder:
        from snapflow.core.streams import StreamBuilder

        return StreamBuilder().filter_inputs([self.input_node.key])

    def as_bound_input(
        self, bound_block: DataBlock = None, bound_stream: DataBlockStream = None
    ) -> BoundInput:
        from snapflow.core.function_interface_manager import BoundInput

        return BoundInput(
            name=self.name,
            input=self.input,
            input_node=self.input_node,
            schema_translation=self.schema_translation,
            bound_block=bound_block,
            bound_stream=bound_stream,
        )


class PythonException(FrozenPydanticBase):
    error: str
    traceback: str

    @classmethod
    def from_exception(cls, e: Exception):
        tback = traceback.format_exc()
        error = str(e) or type(e).__name__  # MUST evaluate true if there's an error!
        # Traceback can be v large (like in max recursion), so we truncate to 5k chars
        tback = tback[:5000]
        return PythonException(error=error, traceback=tback)


class ExecutionResult(PydanticBase):
    input_blocks_consumed: Dict[str, List[DataBlockMetadataCfg]] = {}
    output_blocks_emitted: Dict[str, DataBlockMetadataCfg] = {}
    stored_blocks_created: Dict[str, List[StoredDataBlockMetadataCfg]] = {}
    schemas_generated: List[Schema] = None
    function_error: Optional[PythonException] = None
    framework_error: Optional[PythonException] = None

    def has_error(self) -> bool:
        return self.function_error is not None or self.framework_error is not None
