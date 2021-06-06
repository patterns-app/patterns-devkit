from __future__ import annotations
from snapflow.core.execution.context import DataFunctionContext
from snapflow.core.function import DataFunction

from commonmodel.base import Schema
from snapflow.core.data_block import DataBlock, DataBlockStream
from snapflow.core.persisted.pydantic import (
    DataBlockMetadataCfg,
    DataFunctionLogCfg,
    StoredDataBlockMetadataCfg,
)
from snapflow.core.declarative.interface import BoundInputCfg, BoundInterfaceCfg
from snapflow.core.storage import ensure_data_block_on_storage_cfg
from snapflow.core.declarative.base import FrozenPydanticBase, PydanticBase
from snapflow.core.component import ComponentLibrary, global_library

import traceback
from collections import abc, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from io import BufferedIOBase, BytesIO, IOBase, RawIOBase
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import dcp
import sqlalchemy
from dcp.data_format.base import DataFormat, get_format_for_nickname, DataFormatBase
from dcp.storage.base import FileSystemStorageClass, MemoryStorageClass, Storage
from dcp.utils.common import rand_str, utcnow
from loguru import logger
from snapflow.core.persisted.data_block import (
    Alias,
    DataBlockMetadata,
    StoredDataBlockMetadata,
    get_datablock_id,
    get_stored_datablock_id,
    make_sdb_name,
)
from snapflow.core.declarative.dataspace import ComponentLibraryCfg, DataspaceCfg
from snapflow.core.declarative.execution import (
    ExecutableCfg,
    ExecutionCfg,
    ExecutionResult,
)
from snapflow.core.declarative.function import DEFAULT_OUTPUT_NAME, DataFunctionCfg
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.typing.casting import cast_to_realized_schema


@dataclass
class OutputHandler:
    ctx: DataFunctionContext
    executable: ExecutableCfg
    library: Optional[ComponentLibrary] = None
    result: ExecutionResult

    def create_stored_datablock(self) -> StoredDataBlockMetadataCfg:
        block = DataBlockMetadataCfg(
            id=get_datablock_id(),
            inferred_schema_key=None,
            nominal_schema_key=None,
            realized_schema_key="Any",
            record_count=None,
            created_by_node_key=self.node.key,
        )
        sid = get_stored_datablock_id()
        sdb = StoredDataBlockMetadataCfg(  # type: ignore
            id=sid,
            name=make_sdb_name(sid, self.node.key),
            data_block_id=block.id,
            data_block=block,
            storage_url=self.execution_config.target_storage,
            data_format=UnknownFormat.nickname,
            data_is_written=False,
        )
        return sdb

    def get_stored_datablock_for_output(
        self, output: str
    ) -> StoredDataBlockMetadataCfg:
        db = self.result.output_blocks_emitted.get(output)
        if db is None:
            sdb = self.create_stored_datablock()
            db = sdb.data_block
            self.result.output_blocks_emitted[output] = sdb.data_block
            self.result.stored_blocks_created.setdefault(db.id, []).append(sdb)
        sdbs = self.result.stored_blocks_created[db.id]
        for sdb in sdbs:
            if sdb.storage == self.execution_config.target_storage:
                return sdb
        return sdbs[0]

    def handle_emit(
        self,
        records_obj: Any = None,
        name: str = None,
        storage: Storage = None,
        output: str = DEFAULT_OUTPUT_NAME,
        data_format: DataFormat = None,
        schema: SchemaLike = None,
    ):
        logger.debug(
            f"HANDLING EMITTED OBJECT (of type '{type(records_obj).__name__}')"
        )
        # TODO: can i return an existing DataBlock? Or do I need to create a "clone"?
        #   Answer: ok to return as is (just mark it as 'output' in DBL)
        if isinstance(records_obj, StoredDataBlockMetadata):
            # TODO is it in local storage tho? we skip conversion below...
            # This is just special case right now to support SQL function
            # Will need better solution for explicitly creating DB/SDBs inside of functions
            sdb = records_obj
            records_obj = None
            name = sdb.name
            storage = sdb.storage
        elif isinstance(records_obj, DataBlockMetadata):
            raise NotImplementedError
        elif isinstance(records_obj, DataBlock):
            raise NotImplementedError
        else:
            sdb = self.get_stored_datablock_for_output(output)
        nominal_output_schema = schema
        if nominal_output_schema is None:
            nominal_output_schema = (
                self.executable.bound_interface.resolve_nominal_output_schema()
            )
        if nominal_output_schema is not None:
            nominal_output_schema = self.library.get_schema(nominal_output_schema)
        db = sdb.data_block
        if (
            db.nominal_schema_key
            and db.nominal_schema(self.library).key != nominal_output_schema.key
        ):
            raise Exception(
                "Mismatch nominal schemas {db.nominal_schema_key} - {nominal_output_schema.key}"
            )
        db.nominal_schema_key = nominal_output_schema.key
        if records_obj is not None:
            name, storage = self.handle_python_object(records_obj)
            if nominal_output_schema is not None:
                # TODO: still unclear on when and why to do this cast
                handler = get_handler_for_name(name, storage)
                handler().cast_to_schema(name, storage, nominal_output_schema)
        assert name is not None
        assert storage is not None
        self.append_records_to_stored_datablock(name, storage, sdb)
        sdb.data_is_written = True
        return sdb

    def handle_python_object(self, obj: Any) -> Tuple[str, Storage]:
        name = "_tmp_obj_" + rand_str(10)
        if isinstance(obj, IOBase):
            # Handle file-like by writing to disk first
            file_storages = [
                s
                for s in self.executable.execution_config.get_storages()
                if s.storage_engine.storage_class == FileSystemStorageClass
            ]
            if not file_storages:
                raise Exception(
                    "File-like object returned but no file storage provided."
                    "Add a file storage to the environment: `env.add_storage('file:///....')`"
                )
            if self.executable.execution_config.get_target_storage() in file_storages:
                storage = self.executable.execution_config.get_target_storage()
            else:
                storage = file_storages[0]
            mode = "w"
            if isinstance(obj, (RawIOBase, BufferedIOBase)):
                mode = "wb"
            with storage.get_api().open(name, mode) as f:
                for s in obj:
                    f.write(s)
        else:
            storage = self.executable.execution_config.get_local_storage()
            storage.get_api().put(name, obj)
        return name, storage

    def add_schema(self, schema: Schema):
        if schema not in self.result.schemas_generated:
            self.result.schemas_generated.append(schema)

    def resolve_new_object_with_data_block(
        self, sdb: StoredDataBlockMetadataCfg, name: str, storage: Storage
    ):
        # TOO expensive to infer schema every time, so just do first time
        if sdb.data_block.realized_schema(self.library).key in (
            None,
            "Any",
            "core.Any",
        ):
            handler = get_handler_for_name(name, storage)
            inferred_schema = handler().infer_schema(name, storage)
            logger.debug(
                f"Inferred schema: {inferred_schema.key} {inferred_schema.fields_summary()}"
            )
            self.add_schema(inferred_schema)
            # Cast to nominal if no existing realized schema
            realized_schema = cast_to_realized_schema(
                inferred_schema=inferred_schema,
                nominal_schema=sdb.data_block.nominal_schema,
            )
            logger.debug(
                f"Realized schema: {realized_schema.key} {realized_schema.fields_summary()}"
            )
            self.add_schema(realized_schema)
            sdb.data_block.realized_schema_key = realized_schema.key
        #     # If already a realized schema, conform new inferred schema to existing realized
        #     realized_schema = cast_to_realized_schema(
        #         self.env,
        #         inferred_schema=inferred_schema,
        #         nominal_schema=sdb.data_block.realized_schema(self.env),
        #     )
        if sdb.data_block.nominal_schema_key:
            logger.debug(
                f"Nominal schema: {sdb.data_block.nominal_schema(self.library).key} {sdb.data_block.nominal_schema(self.library).fields_summary()}"
            )

    def append_records_to_stored_datablock(
        self, name: str, storage: Storage, sdb: StoredDataBlockMetadataCfg
    ):
        self.resolve_new_object_with_data_block(sdb, name, storage)
        if sdb.data_format == UnknownFormat:
            if self.execution_config.target_data_format:
                sdb.data_format = get_format_for_nickname(
                    self.execution_config.target_data_format
                )
            else:
                sdb.data_format = sdb.storage.storage_engine.get_natural_format()
            # fmt = infer_format_for_name(name, storage)
            # # if sdb.data_format and sdb.data_format != fmt:
            # #     raise Exception(f"Format mismatch {fmt} - {sdb.data_format}")
            # if fmt is None:
            #     raise Exception(f"Could not infer format {name} on {storage}")
            # sdb.data_format = fmt
        # TODO: make sure this handles no-ops (empty object, same storage)
        # TODO: copy or alias? sometimes we are just moving temp obj to new name, dont need copy
        # to_name = sdb.name
        # if storage == sdb.storage:
        #     # Same storage
        #     if name == to_name:
        #         # Nothing to do
        #         logger.debug("Output already on storage with same name, nothing to do")
        #         return
        #     else:
        #         # Same storage, just new name
        #         # TODO: should be "rename" ideally (as it is if tmp gets deleted we lose it)
        #         logger.debug("Output already on storage, creating alias")
        #         storage.get_api().create_alias(name, to_name)
        #         return
        logger.debug(
            f"Copying output from {name} {storage} to {sdb.name} {sdb.storage} ({sdb.data_format})"
        )
        result = dcp.copy(
            from_name=name,
            from_storage=storage,
            to_name=sdb.name,
            to_storage=sdb.storage,
            to_format=sdb.data_format,
            available_storages=self.execution_config.get_storages(),
            if_exists="append",
        )
        logger.debug(f"Copied {result}")
        logger.debug(f"REMOVING NAME {name}")
        storage.get_api().remove(name)
