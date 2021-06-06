from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from snapflow.core.component import ComponentLibrary, global_library
from snapflow.core.declarative.function import DEFAULT_OUTPUT_NAME
from snapflow.core.function_interface_manager import bind_inputs
from snapflow.core.persisted.pydantic import (
    DataBlockMetadataCfg,
    DataFunctionLogCfg,
    StoredDataBlockMetadataCfg,
)

import traceback
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from commonmodel.base import Schema
from dcp.storage.base import Storage
from snapflow.core.declarative.base import FrozenPydanticBase, PydanticBase
from snapflow.core.declarative.dataspace import ComponentLibraryCfg, DataspaceCfg
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment
from sqlalchemy.sql.expression import select

if TYPE_CHECKING:
    from snapflow.core.declarative.interface import BoundInterfaceCfg


class ExecutionCfg(FrozenPydanticBase):
    dataspace: DataspaceCfg
    target_storage: str
    local_storage: Optional[str] = None
    target_data_format: Optional[str] = None
    storages: List[str] = []
    library_cfg: Optional[ComponentLibraryCfg] = None
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
    bound_interface: BoundInterfaceCfg
    function_log: DataFunctionLogCfg
    result_listener: MetadataExecutionResultListener

    @property
    def node(self) -> GraphCfg:
        return self.graph.get_node(self.node_key)

    def get_library(self) -> ComponentLibrary:
        if self.execution_config.library_cfg is None:
            lib = global_library
        else:
            lib = ComponentLibrary.from_config(self.execution_config.library_cfg)
            lib.merge(global_library)
        return lib


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

    def stdout_blocks_emitted(self) -> List[DataBlockMetadataCfg]:
        return self.output_blocks_emitted.get(DEFAULT_OUTPUT_NAME, [])


@dataclass
class MetadataExecutionResultListener:
    env: Environment
    exe: ExecutableCfg

    def __call__(self, result: ExecutionResult):
        from snapflow.core.run import save_result

        save_result(self.env, self.exe, result)
