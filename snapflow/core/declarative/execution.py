from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from commonmodel.base import Schema
from dcp.storage.base import Storage
from snapflow.core.component import ComponentLibrary, global_library
from snapflow.core.data_block import DataBlock, as_managed
from snapflow.core.declarative.base import FrozenPydanticBase, PydanticBase
from snapflow.core.declarative.dataspace import ComponentLibraryCfg, DataspaceCfg
from snapflow.core.declarative.function import DEFAULT_OUTPUT_NAME
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.declarative.interface import BoundInterfaceCfg
from snapflow.core.environment import Environment
from snapflow.core.function_interface_manager import bind_inputs
from snapflow.core.persistence.pydantic import (
    DataBlockMetadataCfg,
    DataBlockWithStoredBlocksCfg,
    DataFunctionLogCfg,
    StoredDataBlockMetadataCfg,
)
from sqlalchemy.sql.expression import select


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
    stored_blocks_created: Dict[
        str, List[StoredDataBlockMetadataCfg]
    ] = {}  # Keyed on data block ID
    schemas_generated: List[Schema] = []
    function_error: Optional[PythonException] = None
    framework_error: Optional[PythonException] = None  # TODO: do we ever use this?

    def has_error(self) -> bool:
        return self.function_error is not None or self.framework_error is not None

    def stdout_block_emitted(self) -> Optional[DataBlockMetadataCfg]:
        return self.output_blocks_emitted.get(DEFAULT_OUTPUT_NAME)

    def stdout(self) -> Optional[DataBlock]:
        dbc = self.stdout_block_emitted()
        dbws = DataBlockWithStoredBlocksCfg(
            **dbc.dict(), stored_data_blocks=self.stored_blocks_created.get(dbc.id, [])
        )
        return as_managed(dbws)

    def finalize(self, compute_record_counts: bool = True) -> ExecutionResult:
        if compute_record_counts:
            self.compute_record_counts()
        return ExecutionResult(
            input_blocks_consumed=self.input_blocks_consumed,
            output_blocks_emitted={
                n: b for n, b in self.output_blocks_emitted.items() if b.data_is_written
            },
            stored_blocks_created={
                bid: [b for b in blocks if b.data_is_written]
                for bid, blocks in self.stored_blocks_created.items()
            },
            schemas_generated=self.schemas_generated,
            function_error=self.function_error,
            framework_error=self.framework_error,
        )

    def compute_record_counts(self):
        cnts = {}
        for sdbs in self.stored_blocks_created.values():
            if not sdbs:
                continue
            sdb = sdbs[0]  # TODO: choose cheapest sdb?
            cnts[sdb.data_block_id] = (
                Storage(sdb.storage_url).get_api().record_count(sdb.name)
            )
        for db in self.output_blocks_emitted.values():
            db.record_count = cnts.get(db.id)


@dataclass
class MetadataExecutionResultListener:
    env: Environment
    exe: ExecutableCfg

    def __call__(self, result: ExecutionResult):
        from snapflow.core.execution.run import save_result

        with self.env.md_api.begin():
            save_result(self.env, self.exe, result)


# Used for local python runtime
global_metadata_result_listener: Optional[MetadataExecutionResultListener] = None


def get_global_metadata_result_listener() -> Optional[MetadataExecutionResultListener]:
    return global_metadata_result_listener


def set_global_metadata_result_listener(listener: MetadataExecutionResultListener):
    global global_metadata_result_listener
    global_metadata_result_listener = listener


class ExecutableCfg(FrozenPydanticBase):
    node_key: str
    graph: GraphCfg
    execution_config: ExecutionCfg
    bound_interface: BoundInterfaceCfg
    function_log: DataFunctionLogCfg
    result_listener_type: str = MetadataExecutionResultListener.__name__

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
