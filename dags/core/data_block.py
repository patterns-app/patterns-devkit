from __future__ import annotations

from collections import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Optional, Tuple

import sqlalchemy as sa
from pandas import DataFrame
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, event, or_
from sqlalchemy.orm import RelationshipProperty, Session, relationship

from dags.core.data_formats import (
    DataFormat,
    DataFormatType,
    DataFrameFormat,
    get_data_format_of_object,
    get_records_list_sample,
)
from dags.core.data_formats.base import MemoryDataFormat
from dags.core.data_formats.database_table_ref import (
    DatabaseTableRef,
    DatabaseTableRefFormat,
)
from dags.core.data_formats.records_list import RecordsList, RecordsListFormat
from dags.core.environment import Environment
from dags.core.metadata.listeners import immutability_update_listener
from dags.core.metadata.orm import BaseModel, timestamp_rand_key
from dags.core.typing.inference import infer_schema_from_records_list
from dags.core.typing.object_schema import ObjectSchema, ObjectSchemaKey, is_any
from dags.utils.typing import T
from loguru import logger

if TYPE_CHECKING:
    from dags.core.conversion import (
        StorageFormat,
        get_conversion_path_for_sdb,
        convert_sdb,
    )
    from dags.core.runnable import ExecutionContext
    from dags.core.storage.storage import (
        Storage,
        LocalMemoryStorageEngine,
    )


@dataclass(frozen=True)
class LocalMemoryDataRecords:
    data_format: DataFormat
    records_object: Any  # TODO: only eligible types: DataFrame, RecordsList, table name (str)
    record_count: Optional[int] = None

    @classmethod
    def from_records_object(
        cls, obj: Any, data_format: DataFormat = None, record_count: int = None
    ) -> LocalMemoryDataRecords:
        if data_format is None:
            data_format = get_data_format_of_object(obj)
        if data_format is None:
            raise NotImplementedError(obj)
        assert data_format.is_memory_format()
        if record_count is None:
            record_count = data_format.get_record_count(obj)
        return LocalMemoryDataRecords(
            data_format=data_format, records_object=obj, record_count=record_count
        )

    def copy(self) -> LocalMemoryDataRecords:
        records_object = self.data_format.copy_records(self.records_object)
        return LocalMemoryDataRecords(
            data_format=self.data_format,
            record_count=self.record_count,
            records_object=records_object,
        )

    @property
    def record_count_display(self):
        return self.record_count if self.record_count is not None else "Unknown"


class DataBlockMetadata(BaseModel):  # , Generic[DT]):
    # id = Column(String, primary_key=True, default=timestamp_rand_key)
    id = Column(Integer, primary_key=True, autoincrement=True)
    expected_schema_key: ObjectSchemaKey = Column(String, nullable=True)  # type: ignore
    realized_schema_key: ObjectSchemaKey = Column(String, nullable=True)  # type: ignore
    record_count = Column(Integer, nullable=True)
    # Other metadata? created_by_job? last_processed_at?
    deleted = Column(Boolean, default=False)
    data_sets: RelationshipProperty = relationship(
        "DataSetMetadata", backref="data_block"
    )
    stored_data_blocks: RelationshipProperty = relationship(
        "StoredDataBlockMetadata", backref="data_block", lazy="dynamic"
    )
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="data_block"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            expected_schema_key=self.expected_schema_key,
            realized_schema_key=self.realized_schema_key,
        )

    @property
    def most_real_schema_key(self) -> ObjectSchemaKey:
        return self.realized_schema_key or self.expected_schema_key

    @property
    def most_abstract_schema_key(self) -> ObjectSchemaKey:
        return self.expected_schema_key or self.realized_schema_key

    def as_managed_data_block(self, ctx: ExecutionContext):
        mgr = DataBlockManager(ctx, self,)
        return ManagedDataBlock(
            data_block_id=self.id,
            expected_schema_key=self.expected_schema_key,
            realized_schema_key=self.realized_schema_key,
            manager=mgr,
        )

    def created_by(self, sess: Session) -> Optional[str]:
        from dags.core.node import DataBlockLog
        from dags.core.node import PipeLog
        from dags.core.node import Direction

        result = (
            sess.query(PipeLog.node_key)
            .join(DataBlockLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                DataBlockLog.data_block_id == self.id,
            )
            .first()
        )
        if result:
            return result[0]
        return None


@dataclass(frozen=True)
class ManagedDataBlock(Generic[T]):
    data_block_id: int
    expected_schema_key: ObjectSchemaKey
    realized_schema_key: ObjectSchemaKey
    manager: DataBlockManager

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_records_list(self) -> RecordsList:
        return self.manager.as_records_list()

    def as_table(self) -> DatabaseTableRef:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    @property
    def expected_schema(self) -> Optional[ObjectSchema]:
        return self.manager.get_expected_schema()

    @property
    def realized_schema(self) -> Optional[ObjectSchema]:
        return self.manager.get_realized_schema()


DataBlock = ManagedDataBlock


class StoredDataBlockMetadata(BaseModel):
    # id = Column(String, primary_key=True, default=timestamp_rand_key)
    id = Column(Integer, primary_key=True, autoincrement=True)
    data_block_id = Column(Integer, ForeignKey(DataBlockMetadata.id), nullable=False)
    storage_url = Column(String, nullable=False)
    data_format: DataFormat = Column(DataFormatType, nullable=False)  # type: ignore
    # is_ephemeral = Column(Boolean, default=False) # TODO
    # Hints
    data_block: "DataBlockMetadata"

    def __repr__(self):
        return self._repr(
            id=self.id,
            data_block=self.data_block,
            data_format=self.data_format,
            storage_url=self.storage_url,
        )

    def get_realized_schema(self, env: Environment) -> Optional[ObjectSchema]:
        if self.data_block.realized_schema_key is None:
            return None
        return env.get_schema(self.data_block.realized_schema_key)

    def get_expected_schema(self, env: Environment) -> Optional[ObjectSchema]:
        if self.data_block.expected_schema_key is None:
            return None
        return env.get_schema(self.data_block.expected_schema_key)

    @property
    def storage(self) -> Storage:
        from dags.core.storage.storage import Storage

        return Storage.from_url(self.storage_url)

    def get_name(self, env: Environment) -> str:
        if self.data_block_id is None or self.id is None:
            raise ValueError(
                "Trying to get StoredDataBlock name, but id is not set yet (obj must be committed to database first)"
            )
        schema = env.get_schema(self.data_block.most_real_schema_key)
        return f"_{schema.get_identifier()[:50]}_{self.id}"  # TODO: max table name lengths in other engines? (63 in postgres)

    def get_storage_format(self) -> StorageFormat:
        return StorageFormat(self.storage.storage_type, self.data_format)

    def exists(self, env: Environment) -> bool:
        return self.storage.get_manager(env).exists(self)

    def record_count(self, env: Environment) -> Optional[int]:
        if self.data_block.record_count is not None:
            return self.data_block.record_count
        return self.storage.get_manager(env).record_count(self)


event.listen(DataBlockMetadata, "before_update", immutability_update_listener)
event.listen(StoredDataBlockMetadata, "before_update", immutability_update_listener)


class DataSetMetadata(BaseModel):
    # id = Column(String, primary_key=True, default=timestamp_rand_key)
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    expected_schema_key: ObjectSchemaKey = Column(String, nullable=True)  # type: ignore
    realized_schema_key: ObjectSchemaKey = Column(String, nullable=False)  # type: ignore
    data_block_id = Column(Integer, ForeignKey(DataBlockMetadata.id), nullable=False)
    # Hints
    data_block: "DataBlockMetadata"

    # __mapper_args__ = {
    #     "polymorphic_identity": True,
    # }

    def __repr__(self):
        return self._repr(
            id=self.id,
            name=self.name,
            expected_schema_key=self.expected_schema_key,
            realized_schema_key=self.realized_schema_key,
            data_block=self.data_block,
        )

    def as_managed_data_block(self, ctx: ExecutionContext):
        mgr = DataBlockManager(ctx, self.data_block)
        return ManagedDataSet(
            data_set_id=self.id,
            data_set_name=self.name,
            data_block_id=self.data_block_id,
            expected_schema_key=self.expected_schema_key,
            realized_schema_key=self.realized_schema_key,
            manager=mgr,
        )


@dataclass(frozen=True)
class ManagedDataSet(Generic[T]):
    data_set_id: int
    data_set_name: str
    data_block_id: int
    expected_schema_key: ObjectSchemaKey
    realized_schema_key: ObjectSchemaKey
    # schema_is_validated: bool
    manager: DataBlockManager

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_records_list(self) -> RecordsList:
        return self.manager.as_records_list()

    def as_table(self) -> DatabaseTableRef:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    @property
    def expected_schema(self) -> Optional[ObjectSchema]:
        return self.manager.get_expected_schema()

    @property
    def realized_schema(self) -> Optional[ObjectSchema]:
        return self.manager.get_realized_schema()


DataSet = ManagedDataSet


class DataBlockManager:
    def __init__(
        self, ctx: ExecutionContext, data_block: DataBlockMetadata,
    ):

        self.ctx = ctx
        self.data_block = data_block

    def __str__(self):
        return f"DRM: {self.data_block}, Local: {self.ctx.local_memory_storage}, rest: {self.ctx.storages}"

    def get_realized_schema(self) -> Optional[ObjectSchema]:
        if self.data_block.realized_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.realized_schema_key)

    def get_expected_schema(self) -> Optional[ObjectSchema]:
        if self.data_block.expected_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.expected_schema_key)

    def as_dataframe(self) -> DataFrame:
        return self.as_format(DataFrameFormat)

    def as_records_list(self) -> RecordsList:
        return self.as_format(RecordsListFormat)

    def as_table(self) -> DatabaseTableRef:
        return self.as_format(DatabaseTableRefFormat)

    def as_format(self, fmt: DataFormat) -> Any:
        from dags.core.storage.storage import LocalMemoryStorageEngine

        sdb = self.get_or_create_local_stored_data_block(fmt)
        local_memory_storage = LocalMemoryStorageEngine(
            self.ctx.env, self.ctx.local_memory_storage
        )
        return local_memory_storage.get_local_memory_data_records(sdb).records_object

    def get_or_create_local_stored_data_block(
        self, target_format: DataFormat
    ) -> StoredDataBlockMetadata:
        from dags.core.conversion import (
            StorageFormat,
            get_conversion_path_for_sdb,
            convert_sdb,
        )

        cnt = (
            self.ctx.metadata_session.query(StoredDataBlockMetadata)
            .filter(StoredDataBlockMetadata.data_block == self.data_block)
            .count()
        )
        logger.debug(f"{cnt} SDBs available")
        existing_sdbs = self.ctx.metadata_session.query(StoredDataBlockMetadata).filter(
            StoredDataBlockMetadata.data_block == self.data_block,
            # DO NOT fetch memory SDBs that aren't of current runtime (since we can't get them!)
            # TODO: clean up memory SDBs when the memory goes away? Doesn't make sense to persist them really
            # Should be a separate in-memory lookup for memory SDBs, so they naturally expire?
            or_(
                ~StoredDataBlockMetadata.storage_url.startswith("memory:"),
                StoredDataBlockMetadata.storage_url
                == self.ctx.local_memory_storage.url,
            ),
        )
        logger.debug(
            f"{existing_sdbs.count()} SDBs available not in non-local memory (local: {self.ctx.local_memory_storage.url})"
        )
        existing_sdbs.filter(
            StoredDataBlockMetadata.storage_url.in_(self.ctx.all_storages),
        )
        logger.debug(f"{existing_sdbs.count()} SDBs available in eligible storages")
        target_storage_format = StorageFormat(
            self.ctx.local_memory_storage.storage_type, target_format
        )

        # Compute conversion costs
        eligible_conversion_paths = (
            []
        )  #: List[List[Tuple[ConversionCostLevel, Type[Converter]]]] = []
        existing_sdbs = list(existing_sdbs)
        for sdb in existing_sdbs:
            source_storage_format = StorageFormat(
                sdb.storage.storage_type, sdb.data_format
            )
            if source_storage_format == target_storage_format:
                return sdb
            conversion_path = get_conversion_path_for_sdb(
                sdb, target_storage_format, self.ctx.all_storages
            )
            if conversion_path is not None:
                eligible_conversion_paths.append(
                    (conversion_path.total_cost, conversion_path, sdb)
                )
        if not eligible_conversion_paths:
            raise NotImplementedError(
                f"No converter to {target_format} for existing StoredDataBlocks {existing_sdbs}. {self}"
            )
        cost, conversion_path, in_sdb = min(
            eligible_conversion_paths, key=lambda x: x[0]
        )
        return convert_sdb(self.ctx, in_sdb, conversion_path)


def create_data_block_from_records(
    env: Environment,
    sess: Session,
    local_storage: Storage,
    records: Any,
    expected_schema: ObjectSchema = None,
    realized_schema: ObjectSchema = None,
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    from dags.core.storage.storage import LocalMemoryStorageEngine

    if not expected_schema:
        expected_schema = env.get_schema("Any")
    expected_schema_key = expected_schema.key
    ldr = LocalMemoryDataRecords.from_records_object(records)
    if not realized_schema:
        if is_any(expected_schema):
            realized_schema = ldr.data_format.infer_schema_from_records(
                ldr.records_object
            )
            env.add_new_schema(realized_schema)
        else:
            realized_schema = expected_schema
    realized_schema_key = realized_schema.key
    block = DataBlockMetadata(
        expected_schema_key=expected_schema_key,
        realized_schema_key=realized_schema_key,
        record_count=ldr.record_count,
    )
    sdb = StoredDataBlockMetadata(  # type: ignore
        data_block=block, storage_url=local_storage.url, data_format=ldr.data_format,
    )
    sess.add(block)
    sess.add(sdb)
    # Sqlalchemy is a bit finnicky with getting objects live in the right session
    # I don't understand why these merges should be necessary... but they appear to be
    block = sess.merge(block)
    sdb = sess.merge(sdb)
    LocalMemoryStorageEngine(env, local_storage).store_local_memory_data_records(
        sdb, ldr
    )
    return block, sdb
