from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from commonmodel import Schema
from dcp.data_format.base import DataFormat
from dcp.storage.base import Storage
from dcp.utils.common import as_identifier
from pydantic.class_validators import root_validator
from snapflow.core.declarative.base import FrozenPydanticBase
from pydantic import validator


class DataBlockMetadataCfg(FrozenPydanticBase):
    id: str
    # id = Column(Integer, primary_key=True, autoincrement=True)
    realized_schema: Schema
    inferred_schema: Optional[Schema] = None
    nominal_schema: Optional[Schema] = None
    record_count: Optional[int] = None
    created_by_node_key: Optional[str] = None
    deleted: bool = False

    @property
    def manager(self) -> DataBlockManager:
        pass

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_records(self) -> Records:
        return self.manager.as_records()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    def as_table(self, storage: Storage) -> str:
        return self.manager.as_table(storage)

    def as_sql_from_stmt(self, storage: Storage) -> str:
        from snapflow.core.sql.sql_function import apply_schema_translation_as_sql

        # TODO: this feels pretty forced -- how do we do schema transations in a general way for non-memory storages / runtimes?
        sql = self.manager.as_table(storage)
        if self.manager.schema_translation:
            sql = apply_schema_translation_as_sql(
                self.manager.env, sql, self.manager.schema_translation
            )
        return sql

    def has_format(self, fmt: DataFormat) -> bool:
        return self.manager.has_format(fmt)


class StoredDataBlockMetadataCfg(FrozenPydanticBase):
    id: str
    data_block: DataBlockMetadataCfg
    storage: Storage
    name: Optional[str] = None
    data_format: DataFormat
    data_is_written: bool = False

    @root_validator
    def ensure_name(self, values: Dict) -> Dict:
        if not values.get("name"):
            values["name"] = make_sdb_name(
                values["id"], values["data_block"].created_by_node_key
            )
        return values


def make_sdb_name(id: str, node_key: str = None) -> str:
    node_key = node_key or ""
    return as_identifier(f"_{node_key[:30]}_{id}")


class DataBlockManager(FrozenPydanticBase):
    data_block: DataBlockMetadataCfg
    storages: List[Storage]
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

    def ensure_format(self, fmt: DataFormat, target_storage: Storage = None) -> Any:
        from snapflow.core.storage import ensure_data_block_on_storage

        if fmt.natural_storage_class == MemoryStorageClass:
            # Ensure we are putting memory format in memory
            # if not target_storage.storage_engine.storage_class == PythonStorageClass:
            for s in self.storages:
                if s.storage_engine.storage_class == MemoryStorageClass:
                    target_storage = s
        assert target_storage is not None
        sdb = ensure_data_block_on_storage(
            self.env,
            block=self.data_block,
            storage=target_storage,
            fmt=fmt,
            eligible_storages=self.env.get_storages(),
        )
        return sdb

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
        return (
            self.env.md_api.execute(
                select(StoredDataBlockMetadata)
                .filter(StoredDataBlockMetadata.data_block == self.data_block)
                .filter(StoredDataBlockMetadata.data_format == fmt)
            )
        ).scalar_one_or_none() is not None


class DataBlockStream(FrozenPydanticBase):
    blocks: List[DataBlockMetadataCfg] = []
    declared_schema: Optional[Schema] = None
    declared_schema_translation: Optional[Dict[str, str]] = None
    _emitted_blocks: List[DataBlockMetadataCfg] = []
    index: int = 0

    def __iter__(self) -> Iterator[DataBlockMetadataCfg]:
        return self.blocks

    def __next__(self) -> DataBlock:
        if self.index >= len(self.blocks):
            raise StopIteration
        item = self.blocks[self.index]
        self.index += 1
        return item

    def as_managed_block(
        self, stream: Iterator[DataBlockMetadata]
    ) -> Iterator[DataBlock]:
        from snapflow.core.function_interface_manager import get_schema_translation

        for db in stream:
            if db.nominal_schema_key:
                schema_translation = get_schema_translation(
                    self.env,
                    source_schema=db.nominal_schema(self.env),
                    target_schema=self.declared_schema,
                    declared_schema_translation=self.declared_schema_translation,
                )
            else:
                schema_translation = None
            mdb = db.as_managed_data_block(
                self.env, schema_translation=schema_translation
            )
            yield mdb

    @property
    def all_blocks(self) -> List[DataBlock]:
        return self._blocks

    def count(self) -> int:
        return len(self._blocks)

    def log_emitted(self, stream: Iterator[DataBlock]) -> Iterator[DataBlock]:
        for mdb in stream:
            self._emitted_blocks.append(mdb.data_block_metadata)
            self._emitted_managed_blocks.append(mdb)
            yield mdb

    def get_emitted_blocks(self) -> List[DataBlockMetadata]:
        return self._emitted_blocks

    def get_emitted_managed_blocks(self) -> List[DataBlock]:
        return self._emitted_managed_blocks

