from __future__ import annotations
from snapflow.core.persisted.pydantic import (
    DataBlockMetadataCfg,
    DataBlockWithStoredBlocksCfg,
    StoredDataBlockMetadataCfg,
)
from snapflow.core.storage import ensure_data_block_on_storage_cfg
from snapflow.core.declarative.base import FrozenPydanticBase, PydanticBase

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
    Union,
)

import dcp
import sqlalchemy
from commonmodel.base import AnySchema, Schema, SchemaLike, SchemaTranslation
from dcp.data_format.base import DataFormat, get_format_for_nickname
from dcp.storage.base import FileSystemStorageClass, MemoryStorageClass, Storage
from dcp.utils.common import rand_str, utcnow
from loguru import logger
from snapflow.core.persisted.data_block import StoredDataBlockMetadata

if TYPE_CHECKING:
    from snapflow.core.declarative.context import DataFunctionContext


class DataBlockManager:
    def __init__(
        self,
        ctx: DataFunctionContext,
        data_block: DataBlockWithStoredBlocksCfg,
        schema_translation: Optional[SchemaTranslation] = None,
    ):
        self.ctx = ctx
        self.data_block = data_block
        self.stored_data_blocks = self.data_block.stored_data_blocks
        self.storages = self.ctx.execution_config.storages
        self.schema_translation = schema_translation

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


DataBlock = DataBlockManager
SelfReference = Union[DataBlock, None]
Reference = DataBlock
Consumable = DataBlock


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

