from __future__ import annotations
from snapflow.core.storage import ensure_data_block_on_storage_cfg
from snapflow.core.declarative.data_block import (
    DataBlockMetadataCfg,
    StoredDataBlockMetadataCfg,
)
from snapflow.core.declarative.base import FrozenPydanticBase, PydanticBase
from snapflow.core.component import ComponentLibrary

import traceback
from collections import abc, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from io import BufferedIOBase, BytesIO, IOBase, RawIOBase
from typing import (
    Iterable,
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

import dcp
import sqlalchemy
from commonmodel.base import AnySchema, Schema, SchemaLike, SchemaTranslation
from dcp.data_format.base import DataFormat, get_format_for_nickname
from dcp.data_format.handler import get_handler_for_name, infer_format_for_name
from dcp.storage.base import FileSystemStorageClass, MemoryStorageClass, Storage
from dcp.utils.common import rand_str, utcnow
from loguru import logger
from snapflow.core.data_block import (
    Alias,
    DataBlock,
    DataBlockMetadata,
    ManagedDataBlock,
    StoredDataBlockMetadata,
    get_datablock_id,
    get_stored_datablock_id,
)
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.execution import (
    ExecutableCfg,
    ExecutionCfg,
    ExecutionResult,
    NodeInputCfg,
)
from snapflow.core.declarative.function import DEFAULT_OUTPUT_NAME
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment
from snapflow.core.function import (
    DataFunction,
    DataInterfaceType,
    InputExhaustedException,
)
from snapflow.core.function_interface_manager import BoundInput, BoundInterface
from snapflow.core.metadata.api import MetadataApi
from snapflow.core.state import DataBlockLog, DataFunctionLog, Direction, get_state
from snapflow.core.typing.casting import cast_to_realized_schema
from snapflow.utils.output import cf, error_symbol, success_symbol
from sqlalchemy.sql.expression import select


def run_dataspace(ds: DataspaceCfg):
    env = Environment(ds)
    env.run_graph(ds.graph)


class ImproperlyStoredDataBlockException(Exception):
    pass


def validate_data_blocks(env: Environment):
    # TODO: More checks?
    env.md_api.flush()
    for obj in env.md_api.active_session.identity_map.values():
        if isinstance(obj, DataBlockMetadata):
            urls = set([sdb.storage_url for sdb in obj.stored_data_blocks])
            if all(u.startswith("python") for u in urls):
                fmts = set([sdb.data_format for sdb in obj.stored_data_blocks])
                if all(not f.is_storable() for f in fmts):
                    raise ImproperlyStoredDataBlockException(
                        f"DataBlock {obj} is not properly stored (no storeable format(s): {fmts})"
                    )


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


class ExecutionManager:
    def __init__(self, ctx: DataFunctionContext):
        self.ctx = ctx
        self.logger = ExecutionLogger()
        self.node = self.ctx.node
        self.function = ctx.library.get_function(self.node.function)
        self.start_time = utcnow()

    def execute(self) -> ExecutionResult:
        # Setup for run
        base_msg = (
            f"Running node {cf.bold(self.node.key)} {cf.dimmed(self.function.key)}\n"
        )
        logger.debug(
            f"RUNNING NODE {self.node.key} {self.function.key} with params `{self.node.params}`"
        )
        logger.debug(self.ctx)
        self.logger.log(base_msg)
        with self.logger.indent():
            self._execute()
            result = self.ctx.result
            self.log_execution_result(result)
            if not result.has_error():
                self.logger.log(cf.success("Ok " + success_symbol + "\n"))  # type: ignore
            else:
                error = result.function_error or "DataFunction failed (unknown error)"
                self.logger.log(cf.error("Error " + error_symbol + " " + cf.dimmed(error[:80])) + "\n")  # type: ignore
                if result.function_error.traceback:
                    self.logger.log(cf.dimmed(result.function_error.traceback), indent=2)  # type: ignore
            logger.debug(f"Execution result: {result}")
            logger.debug(f"*DONE* RUNNING NODE {self.node.key} {self.function.key}")
        return result

    def _execute(self) -> ExecutionResult:
        function_args, function_kwargs = self.ctx.get_function_args()
        output_obj = self.ctx.function.function_callable(
            *function_args, **function_kwargs,
        )
        if output_obj is not None:
            self.emit_output_object(output_obj)
            # TODO: update node state block counts?
        result = self.ctx.result
        logger.debug(f"EXECUTION RESULT {result}")
        return result

    def emit_output_object(self, output_obj: DataInterfaceType):
        assert output_obj is not None
        if isinstance(output_obj, abc.Generator):
            output_iterator = output_obj
        else:
            output_iterator = [output_obj]
        i = 0
        for output_obj in output_iterator:
            logger.debug(output_obj)
            i += 1
            self.ctx.emit(output_obj)

    def log_execution_result(self, result: ExecutionResult):
        self.logger.log("Inputs: ")
        if result.input_block_counts:
            self.logger.log("\n")
            with self.logger.indent():
                for input_name, cnt in result.input_block_counts.items():
                    self.logger.log(f"{input_name}: {cnt} block(s) processed\n")
        else:
            if not result.non_reference_inputs_bound:
                self.logger.log_token("n/a\n")
            else:
                self.logger.log_token("None\n")
        self.logger.log("Outputs: ")
        if result.output_blocks:
            self.logger.log("\n")
            with self.logger.indent():
                for output_name, block_summary in result.output_blocks.items():
                    self.logger.log(f"{output_name}:")
                    cnt = block_summary["record_count"]
                    alias = block_summary["alias"]
                    if cnt is not None:
                        self.logger.log_token(f" {cnt} records")
                    self.logger.log_token(
                        f" {alias} " + cf.dimmed(f"({block_summary['id']})\n")  # type: ignore
                    )
        else:
            self.logger.log_token("None\n")


class DataBlockManager(FrozenPydanticBase):
    ctx: DataFunctionContext
    data_block: DataBlockMetadataCfg
    stored_data_blocks: List[StoredDataBlockMetadataCfg] = []
    storages: List[Storage] = []
    schema_translation: Optional[SchemaTranslation] = None

    def as_dataframe(self) -> DataFrame:
        return self.as_format(DataFrameFormat)

    def as_records(self) -> Records:
        return self.as_format(RecordsFormat)

    def as_table(self, storage: Storage) -> str:
        return self.as_format(DatabaseTableFormat, storage)

    def as_format(self, fmt: DataFormat, storage: Storage = None) -> Any:
        sdb = self.ensure_format(fmt, storage)
        return self.as_python_object(sdb)

    def ensure_format(
        self, fmt: DataFormat, target_storage: Storage = None
    ) -> StoredDataBlockMetadataCfg:
        from snapflow.core.storage import ensure_data_block_on_storage

        if fmt.natural_storage_class == MemoryStorageClass:
            # Ensure we are putting memory format in memory
            # if not target_storage.storage_engine.storage_class == PythonStorageClass:
            for s in self.storages:
                if s.storage_engine.storage_class == MemoryStorageClass:
                    target_storage = s
        assert target_storage is not None
        sdbs = ensure_data_block_on_storage_cfg(
            block=self.data_block,
            storage=target_storage,
            stored_blocks=self.stored_data_blocks,
            fmt=fmt,
            eligible_storages=self.storages,
        )
        for sdb in sdbs:
            self.ctx.add_stored_data_block(sdb)
        return sdbs[0]

    def as_python_object(self, sdb: StoredDataBlockMetadata) -> Any:
        if self.schema_translation:
            sdb.get_handler().apply_schema_translation(
                sdb.get_name_for_storage(), sdb.storage, self.schema_translation
            )
        if sdb.data_format.natural_storage_class == MemoryStorageClass:
            obj = sdb.storage.get_api().get(sdb.get_name_for_storage())
        else:
            if sdb.data_format == DatabaseTableFormat:
                # TODO:
                # obj = DatabaseTableRef(sdb.get_name(), storage_url=sdb.storage.url)
                # raise NotImplementedError
                return sdb.get_name_for_storage()
            else:
                # TODO: what is general solution to this? if we do DataFormat.as_python_object(sdb) then
                #       we have formats depending on StoredDataBlocks again, and no seperation of concerns
                #       BUT what other option is there? Need knowledge of storage url and name to make useful "pointer" object
                # raise NotImplementedError(
                #     f"Don't know how to bring '{sdb.data_format}' into python'"
                # )
                return sdb.get_name_for_storage()
        return obj

    def has_format(self, fmt: DataFormat) -> bool:
        return fmt in [s.data_format for s in self.stored_data_blocks]


class DataBlockStream(PydanticBase):
    ctx: DataFunctionContext
    blocks: List[DataBlockMetadataCfg] = []
    declared_schema: Optional[Schema] = None
    declared_schema_translation: Optional[Dict[str, str]] = None
    _managed_blocks: Optional[Iterator[DataBlock]] = None
    _emitted_blocks: List[DataBlockMetadataCfg] = []
    _emitted_managed_blocks: List[DataBlock] = []

    def managed_blocks(self) -> Iterator[DataBlock]:
        if self._managed_blocks is None:
            self._managed_blocks = self.as_managed_block(self.blocks)
        return self._managed_blocks

    def __iter__(self) -> Iterator[DataBlock]:
        return self.managed_blocks()

    def __next__(self) -> DataBlock:
        return next(self.managed_blocks())

    def _as_managed_block(
        self, stream: Iterable[DataBlockMetadataCfg]
    ) -> Iterator[DataBlock]:
        from snapflow.core.function_interface_manager import get_schema_translation

        for db in stream:
            if db.nominal_schema_key:
                schema_translation = get_schema_translation(
                    source_schema=db.nominal_schema(self.ctx.library),
                    target_schema=self.declared_schema,
                    declared_schema_translation=self.declared_schema_translation,
                )
            else:
                schema_translation = None
            mdb = db.as_managed_data_block(
                self.ctx, schema_translation=schema_translation
            )
            yield mdb

    def as_managed_block(
        self, stream: Iterable[DataBlockMetadataCfg]
    ) -> Iterator[DataBlock]:
        return self.log_emitted(self._as_managed_block(self.blocks))

    @property
    def all_blocks(self) -> List[DataBlock]:
        return list(self.as_managed_block(self.blocks))

    def count(self) -> int:
        return len(self.blocks)

    def log_emitted(self, stream: Iterator[DataBlock]) -> Iterator[DataBlock]:
        for mdb in stream:
            self._emitted_blocks.append(mdb.data_block_metadata)
            self._emitted_managed_blocks.append(mdb)
            yield mdb

    def get_emitted_blocks(self) -> List[DataBlockMetadataCfg]:
        return self._emitted_blocks

    def get_emitted_managed_blocks(self) -> List[DataBlock]:
        return self._emitted_managed_blocks

