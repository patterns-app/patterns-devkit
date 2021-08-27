from __future__ import annotations

import traceback
from collections import OrderedDict, abc, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from io import BufferedIOBase, BytesIO, IOBase, RawIOBase
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import dcp
import sqlalchemy
from basis.core.component import ComponentLibrary, global_library
from basis.core.declarative.execution import (
    ExecutableCfg,
    ExecutionCfg,
    ExecutionResult,
)
from basis.core.declarative.function import (
    DEFAULT_ERROR_NAME,
    DEFAULT_OUTPUT_NAME,
    DEFAULT_STATE_NAME,
    FunctionCfg,
    IoBaseCfg,
    is_record_like,
)
from basis.core.declarative.interface import resolve_nominal_output_schema
from basis.core.environment import Environment
from basis.core.execution.executable import Executable, instantiate_executable
from basis.core.function import Function
from basis.core.node import Node
from basis.core.persistence.block import get_block_id  # get_stored_block_id,
from basis.core.persistence.pydantic import (
    BlockMetadataCfg,
    BlockWithStoredBlocksCfg,
    ExecutionLogCfg,
    StoredBlockMetadataCfg,
)
from basis.core.typing.casting import cast_to_realized_schema
from commonmodel.base import Schema
from dcp.data_format import get_handler_for_name
from dcp.data_format.base import (
    DataFormat,
    DataFormatBase,
    UnknownFormat,
    get_format_for_nickname,
)
from dcp.data_format.inference import is_generated_schema
from dcp.storage.base import (
    FileSystemStorageClass,
    MemoryStorageClass,
    Storage,
    ensure_storage,
)
from dcp.utils.common import rand_str, utcnow
from loguru import logger


@dataclass
class OutputBase:
    output: IoBaseCfg
    block_id: str
    records_obj: Optional[Any] = None
    data_format: Optional[DataFormat] = None
    nominal_schema: Optional[str] = None
    create_alias_only: bool = False


@dataclass
class PythonObjectOutput(OutputBase):
    records_obj: Optional[Any] = None


@dataclass
class ExistingStoredOutput(OutputBase):
    name: Optional[str] = None
    storage: Optional[Storage] = None


@dataclass(frozen=True)
class Context:
    env: Environment
    function: Function
    node: Node
    inputs: OrderedDict[str, List[BlockWithStoredBlocksCfg]]
    executable: Executable
    result: ExecutionResult
    execution_start_time: Optional[datetime] = None
    library: Optional[ComponentLibrary] = None

    @property
    def execution_cfg(self) -> ExecutionCfg:
        return self.executable.execution_cfg

    ### Consumption

    def get_records(self, input_name: str) -> List[Record]:
        return self.inputs.get(input_name, [])

    def get_table(self, input_name: str) -> Optional[Table]:
        blocks = self.inputs.get(input_name)
        if not blocks:
            return None
        return blocks[-1]

    def get_state(self) -> Dict:
        t = self.get_table(DEFAULT_STATE_NAME)
        if not t:
            return {}
        return t[0]

    def get_state_value(self, key: str) -> Any:
        self.get_state().get(key)

    def consume(self, input_name: str, obj: Union[Record, Iterable[Records]]):
        if isinstance(obj, BlockMetadataCfg):
            obj = [obj]
        self.result.input_blocks_consumed.setdefault(input_name, []).extend(obj)

    ### Emission

    def emit_block(self, output_name: str, block: BlockWithStoredBlocksCfg):
        self.result.output_blocks_emitted.setdefault(output_name, []).append(block)

    def emit(self, obj: Any, *args, **kwargs):
        if is_record_like(obj):
            self.emit_record(obj, *args, **kwargs)
        else:
            self.emit_table(obj, *args, **kwargs)

    def emit_record(
        self,
        record_obj: Any,
        output_name: str = None,
        schema: Union[str, Schema, None] = None,
    ):
        block = self._create_block(output_name, schema)
        handler = self._get_output_handler(block)
        handler.handle_python_object_stream_output(record_obj)
        self.emit_block(output_name or DEFAULT_OUTPUT_NAME, block)

    def emit_table(
        self,
        table_obj: Any = None,
        output_name: str = None,
        schema: Union[str, Schema, None] = None,
        table_name: str = None,  # TODO: if produced on a storage
        storage: Union[str, Storage] = None,  # TODO: if produced on a storage
        data_format: Union[str, DataFormat] = None,
    ):
        # Create new empty block
        block = self._create_block(output_name, schema)
        # Put records on target storage
        handler = self._get_output_handler(block)
        if table_obj:
            result = handler.handle_python_object_table_output(table_obj)
        elif storage and table_name:
            result = handler.handle_existing_stored_table_output(
                table_name, ensure_storage(storage)
            )
        else:
            raise Exception("Must pass object or table name and storage")
        # Handle schemas
        block.inferred_schema_key = result.inferred_schema.key
        block.realized_schema_key = result.realized_schema.key
        self._save_schema(result.inferred_schema)
        self._save_schema(result.realized_schema)
        self.emit_block(output_name or DEFAULT_OUTPUT_NAME, block)

    def emit_records(
        self,
        records_obj: Iterable[Any],
        output_name: str = None,
        schema: Union[str, Schema, None] = None,
    ):
        for r in records_obj:
            self.emit_record(r, output_name, schema)

    def emit_error(self, error_obj: Any, error_msg: str):
        self.emit(error_obj, output_name=DEFAULT_ERROR_NAME)

    def emit_state(self, state: Dict):
        self.emit_table([state], output_name=DEFAULT_STATE_NAME)

    def emit_state_value(self, key: str, value: Any):
        # TODO: get latest state (even one emitted here!)
        state = self.get_state()
        state[key] = value
        self.emit_state(state)

    ### Params

    def get_param(self, key: str, default: Any = None) -> Any:
        if default is None:
            try:
                default = self.function.get_param(key).default
            except KeyError:
                pass
        return self.node.params.get(key, default)

    def get_params(self, defaults: Dict[str, Any] = None) -> Dict[str, Any]:
        final_params = {
            p.name: p.default for p in self.function.get_interface().parameters.values()
        }
        final_params.update(defaults or {})
        final_params.update(self.node.params)
        return final_params

    ### Other

    def should_continue(self) -> bool:
        """
        Long running functions should check this function periodically
        to honor time limits.
        """
        if (
            not self.execution_cfg.execution_timelimit_seconds
            or not self.execution_start_time
        ):
            return True
        seconds_elapsed = (utcnow() - self.execution_start_time).total_seconds()
        should = seconds_elapsed < self.execution_cfg.execution_timelimit_seconds
        if not should:
            self.result.timed_out = True
            logger.debug(
                f"Execution timed out after {self.execution_cfg.execution_timelimit_seconds} seconds"
            )
        return should

    ### Handle output

    def _create_block(
        self, output_name: str = None, nominal_schema=None
    ) -> BlockWithStoredBlocksCfg:
        block = BlockWithStoredBlocksCfg(
            id=get_block_id(self.node.key, output_name),
            created_at=utcnow(),
            updated_at=utcnow(),
            inferred_schema_key=None,
            nominal_schema_key=self._get_nominal_output_schema_key(
                output_name, nominal_schema
            ),
            realized_schema_key="Any",
            record_count=None,
            created_by_node_key=self.node.key,
        )
        sid = get_block_id(
            self.node.key, output_name
        )  # Unique id per stored block (might have multiple formats on same storage)
        stored = StoredBlockMetadataCfg(  # type: ignore
            id=sid,
            created_at=utcnow(),
            updated_at=utcnow(),
            block_id=block.id,
            block=block,
            storage_url=self.execution_cfg.target_storage,
            data_format=UnknownFormat.nickname,
            data_is_written=False,
        )
        block.stored_blocks.append(stored)
        return block

    def _get_output_handler(self, block: BlockWithStoredBlocksCfg):
        handler = OutputHandler(
            executable=self.executable,
            target_name=block.stored_blocks[0].name,
            target_storage=Storage(self.execution_cfg.target_storage),
            target_schema=self.library.get_schema(block.nominal_schema_key)
            if block.nominal_schema_key
            else None,
        )
        if self.execution_cfg.target_data_format:
            handler.target_format = get_format_for_nickname(
                self.execution_cfg.target_data_format
            )
        return handler

    def _get_nominal_output_schema_key(
        self, output_name: str = None, schema: Union[str, Schema, None] = None
    ) -> Optional[str]:
        if schema:
            if isinstance(schema, Schema):
                return schema.key
            return schema
        return resolve_nominal_output_schema(
            self.executable.node.get_interface(), self.inputs, output_name
        )

    def _save_schema(self, schema: Schema):
        if is_generated_schema(schema):
            self.result.schemas_generated.append(schema)


@dataclass
class OutputHandlerResult:
    inferred_schema: Schema
    realized_schema: Schema


@dataclass
class OutputHandler:
    executable: Executable
    target_name: str
    target_storage: Storage
    target_format: Optional[DataFormat] = None
    target_schema: Optional[Schema] = None
    temp_prefix: str = "_tmp_obj_"

    def get_temp_name(self) -> str:
        name = self.temp_prefix + rand_str(10)
        return name

    def handle_existing_stored_table_output(
        self, name: str, storage: Storage
    ) -> OutputHandlerResult:
        # Copy to target_storage
        logger.debug(
            f"Copying output from {name} {storage} to {self.target_name} {self.target_storage.url} ({self.target_format})"
        )
        inferred_schema = self.infer_schema(name, storage)
        realized_schema = self.realize_schema(inferred_schema)
        try:
            result = dcp.copy(
                from_name=name,
                from_storage=storage,
                to_name=self.target_name,
                to_storage=self.target_storage,
                to_format=self.target_format,
                schema=realized_schema,
                available_storages=self.executable.execution_cfg.get_storages(),
                if_exists="error",
            )
        finally:
            # Make sure we delete tmp obj no matter what
            if name.startswith(self.temp_prefix):
                logger.debug(f"REMOVING NAME {name}")
                storage.get_api().remove(name)
        logger.debug(f"Copied {result}")
        return OutputHandlerResult(
            inferred_schema=inferred_schema,
            realized_schema=realized_schema,
        )

    def handle_python_object_stream_output(
        self,
        obj: Any,
    ):
        assert isinstance(obj, dict)  # TODO: only handling dicts for now
        api = self.target_storage.get_api()
        if not isinstance(obj, list):
            obj = [obj]  # Turn into list so is proper datablock?
        # assert target_storage.storage_engine.storage_class == KeyValueStorageEngine
        self.target_storage.get_api().put(self.target_name, obj)

    def handle_python_object_table_output(
        self,
        obj: Any,
    ) -> OutputHandlerResult:
        name, storage = self.put_python_object_on_any_storage(obj)
        return self.handle_existing_stored_table_output(name, storage)

    def put_python_object_on_any_storage(self, obj: Any) -> Tuple[str, Storage]:
        if isinstance(obj, IOBase):
            # Handle file-like by writing to disk first
            return self.put_file_object_on_file_storage(obj)
        storage = self.executable.execution_cfg.get_local_storage()
        assert storage is not None
        name = self.get_temp_name()
        storage.get_api().put(name, obj)
        return name, storage

    def put_file_object_on_file_storage(self, obj: Any) -> Tuple[str, Storage]:
        storage = self.get_file_storage()
        mode = "w"
        if isinstance(obj, (RawIOBase, BufferedIOBase)):
            mode = "wb"
        name = self.get_temp_name()
        with storage.get_api().open(name, mode) as f:
            for s in obj:
                f.write(s)
        return name, storage

    def get_file_storage(self) -> Storage:
        file_storages = [
            s
            for s in self.executable.execution_cfg.get_storages()
            if s.storage_engine.storage_class == FileSystemStorageClass
        ]
        if not file_storages:
            raise Exception(
                "File-like object returned but no file storage provided."
                "Add a file storage to the environment: `env.add_storage('file:///....')`"
            )
        if self.executable.execution_cfg.get_target_storage() in file_storages:
            storage = self.executable.execution_cfg.get_target_storage()
        else:
            storage = file_storages[0]
        return storage

    def infer_schema(self, name: str, storage: Storage) -> Schema:
        handler = get_handler_for_name(name, storage)()
        inferred_schema = handler.infer_schema(name, storage)
        logger.debug(
            f"Inferred schema: {inferred_schema.key} {inferred_schema.fields_summary()}"
        )
        return inferred_schema

    def realize_schema(self, inferred_schema: Schema) -> Schema:
        if self.target_schema:
            realized_schema = cast_to_realized_schema(
                inferred_schema=inferred_schema,
                nominal_schema=self.target_schema,
            )
        else:
            realized_schema = inferred_schema
        logger.debug(
            f"Realized schema: {realized_schema.key} {realized_schema.fields_summary()}"
        )
        return realized_schema
