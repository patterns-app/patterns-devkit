from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Optional, Tuple, Type

from loguru import logger
from pandas import DataFrame
from snapflow.core.data_formats import (
    DataFormat,
    DataFormatType,
    DataFrameFormat,
    get_data_format_of_object,
)
from snapflow.core.data_formats.database_table_ref import (
    DatabaseTableRef,
    DatabaseTableRefFormat,
)
from snapflow.core.data_formats.records_list import RecordsList, RecordsListFormat
from snapflow.core.environment import Environment
from snapflow.core.metadata.orm import BaseModel, timestamp_increment_key
from snapflow.core.typing.casting import cast_to_realized_schema
from snapflow.core.typing.schema import Schema, SchemaKey, SchemaLike, SchemaTranslation
from snapflow.utils.common import as_identifier
from snapflow.utils.typing import T
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, event, or_
from sqlalchemy.orm import RelationshipProperty, Session, relationship

if TYPE_CHECKING:
    from snapflow.core.conversion import (
        StorageFormat,
        get_conversion_path_for_sdb,
        convert_sdb,
    )
    from snapflow.core.runnable import ExecutionContext
    from snapflow.core.storage.storage import (
        Storage,
        LocalMemoryStorageEngine,
    )


@dataclass(frozen=True)
class LocalMemoryDataRecords:
    data_format: DataFormat
    records_object: Any  # TODO: only eligible types: DataFrame, RecordsList, table name (str)
    record_count: Optional[int] = None
    nominal_schema: Optional[SchemaLike] = None

    @classmethod
    def from_records_object(
        cls,
        obj: Any,
        data_format: DataFormat = None,
        record_count: int = None,
        nominal_schema: Optional[SchemaLike] = None,
    ) -> LocalMemoryDataRecords:
        if data_format is None:
            data_format = get_data_format_of_object(obj)
        if data_format is None:
            raise NotImplementedError(obj)
        assert data_format.is_memory_format()
        if record_count is None:
            record_count = data_format.get_record_count(obj)
        return LocalMemoryDataRecords(
            data_format=data_format,
            records_object=obj,
            record_count=record_count,
            nominal_schema=nominal_schema,
        )

    def copy(self) -> LocalMemoryDataRecords:
        records_object = self.data_format.copy_records(self.records_object)
        return LocalMemoryDataRecords(
            data_format=self.data_format,
            record_count=self.record_count,
            records_object=records_object,
            nominal_schema=self.nominal_schema,
        )

    @property
    def record_count_display(self):
        return self.record_count if self.record_count is not None else "Unknown"


DataRecordsObject = LocalMemoryDataRecords


def as_records(
    records_object: Any,
    data_format: Type = None,
    record_count: int = None,
    schema: SchemaLike = None,
) -> DataRecordsObject:
    return LocalMemoryDataRecords.from_records_object(
        records_object,
        data_format=data_format,
        record_count=record_count,
        nominal_schema=schema,
    )


class DataBlockMetadata(BaseModel):  # , Generic[DT]):
    # NOTE on block ids: we generate them dynamically so we don't have to hit a central db for a sequence
    # BUT we MUST ensure they are monotonically ordered -- the logic of selecting the correct (most recent)
    # block relies on strict monotonic IDs in some scenarios
    id = Column(String(128), primary_key=True, default=timestamp_increment_key)
    # id = Column(Integer, primary_key=True, autoincrement=True)
    inferred_schema_key: SchemaKey = Column(String(128), nullable=True)  # type: ignore
    nominal_schema_key: SchemaKey = Column(String(128), nullable=True)  # type: ignore
    realized_schema_key: SchemaKey = Column(String(128), nullable=False)  # type: ignore
    record_count = Column(Integer, nullable=True)
    created_by_node_key = Column(String(128), nullable=True)
    # Other metadata? created_by_job? last_processed_at?
    deleted = Column(Boolean, default=False)
    stored_data_blocks: RelationshipProperty = relationship(
        "StoredDataBlockMetadata", backref="data_block", lazy="dynamic"
    )
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="data_block"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            inferred_schema_key=self.inferred_schema_key,
            nominal_schema_key=self.nominal_schema_key,
            realized_schema_key=self.realized_schema_key,
        )

    def inferred_schema(self, env: Environment) -> Optional[Schema]:
        return env.get_schema(self.inferred_schema_key)

    def nominal_schema(self, env: Environment) -> Optional[Schema]:
        return env.get_schema(self.nominal_schema_key)

    def realized_schema(self, env: Environment) -> Schema:
        return env.get_schema(self.realized_schema_key)

    def as_managed_data_block(
        self,
        ctx: ExecutionContext,
        schema_translation: Optional[SchemaTranslation] = None,
    ):
        mgr = DataBlockManager(ctx, self, schema_translation=schema_translation)
        return ManagedDataBlock(
            data_block_id=self.id,
            inferred_schema_key=self.inferred_schema_key,
            nominal_schema_key=self.nominal_schema_key,
            realized_schema_key=self.realized_schema_key,
            manager=mgr,
        )

    def created_by(self, sess: Session) -> Optional[str]:
        from snapflow.core.node import DataBlockLog
        from snapflow.core.node import PipeLog
        from snapflow.core.node import Direction

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
    data_block_id: str
    inferred_schema_key: SchemaKey
    nominal_schema_key: SchemaKey
    realized_schema_key: SchemaKey
    manager: DataBlockManager

    @property
    def data_block_metadata(self) -> DataBlockMetadata:
        return self.manager.data_block

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_records_list(self) -> RecordsList:
        return self.manager.as_records_list()

    def as_table(self) -> DatabaseTableRef:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    def as_table_stmt(self) -> str:
        return self.as_table().get_table_stmt()

    @property
    def inferred_schema(self) -> Optional[Schema]:
        return self.manager.inferred_schema()

    @property
    def nominal_schema(self) -> Optional[Schema]:
        return self.manager.nominal_schema()

    @property
    def realized_schema(self) -> Schema:
        return self.manager.realized_schema()


DataBlock = ManagedDataBlock


class StoredDataBlockMetadata(BaseModel):
    id = Column(String(128), primary_key=True, default=timestamp_increment_key)
    # id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=True)
    data_block_id = Column(
        String(128), ForeignKey(DataBlockMetadata.id), nullable=False
    )
    storage_url = Column(String(128), nullable=False)
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

    def inferred_schema(self, env: Environment) -> Optional[Schema]:
        if self.data_block.inferred_schema_key is None:
            return None
        return env.get_schema(self.data_block.inferred_schema_key)

    def nominal_schema(self, env: Environment) -> Optional[Schema]:
        if self.data_block.nominal_schema_key is None:
            return None
        return env.get_schema(self.data_block.nominal_schema_key)

    def realized_schema(self, env: Environment) -> Schema:
        if self.data_block.realized_schema_key is None:
            return None
        return env.get_schema(self.data_block.realized_schema_key)

    @property
    def storage(self) -> Storage:
        from snapflow.core.storage.storage import Storage

        return Storage.from_url(self.storage_url)

    def get_name(self, env: Environment) -> str:
        if self.name:
            return self.name
        if self.data_block_id is None or self.id is None:
            env.session.flush([self])
            self = env.session.merge(self)
        node_key = self.data_block.created_by_node_key or ""
        self.name = as_identifier(f"_{node_key[:40]}_{self.id}")
        env.session.flush([self])
        return self.name

    def get_storage_format(self) -> StorageFormat:
        return StorageFormat(self.storage.storage_type, self.data_format)

    def exists(self, env: Environment) -> bool:
        return self.storage.get_manager(env).exists(self)

    def record_count(self, env: Environment) -> Optional[int]:
        if self.data_block.record_count is not None:
            return self.data_block.record_count
        return self.storage.get_manager(env).record_count(self)

    def create_alias(self, env: Environment, alias: str) -> Alias:
        a = Alias(
            alias=alias,
            data_block_id=self.data_block_id,
            stored_data_block_id=self.id,
        )
        self.storage.get_manager(env).create_alias(self, alias)
        return a


# event.listen(DataBlockMetadata, "before_update", immutability_update_listener)
# event.listen(StoredDataBlockMetadata, "before_update", immutability_update_listener)


class Alias(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = Column(String(128))
    data_block_id = Column(
        String(128), ForeignKey(DataBlockMetadata.id), nullable=False
    )
    stored_data_block_id = Column(
        String(128), ForeignKey(StoredDataBlockMetadata.id), nullable=False
    )
    # Hints
    data_block: "DataBlockMetadata"


class DataBlockManager:
    def __init__(
        self,
        ctx: ExecutionContext,
        data_block: DataBlockMetadata,
        schema_translation: Optional[SchemaTranslation] = None,
    ):

        self.ctx = ctx
        self.data_block = data_block
        self.schema_translation = schema_translation

    def __str__(self):
        return f"DRM: {self.data_block}, Local: {self.ctx.local_memory_storage}, rest: {self.ctx.storages}"

    def inferred_schema(self) -> Optional[Schema]:
        if self.data_block.inferred_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.inferred_schema_key)

    def nominal_schema(self) -> Optional[Schema]:
        if self.data_block.nominal_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.nominal_schema_key)

    def realized_schema(self) -> Schema:
        if self.data_block.realized_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.realized_schema_key)

    def as_dataframe(self) -> DataFrame:
        return self.as_format(DataFrameFormat)

    def as_records_list(self) -> RecordsList:
        return self.as_format(RecordsListFormat)

    def as_table(self) -> DatabaseTableRef:
        return self.as_format(DatabaseTableRefFormat)

    def as_format(self, fmt: DataFormat) -> Any:
        from snapflow.core.storage.storage import LocalMemoryStorageEngine

        sdb = self.get_or_create_local_stored_data_block(fmt)
        local_memory_storage = LocalMemoryStorageEngine(
            self.ctx.env, self.ctx.local_memory_storage
        )
        obj = local_memory_storage.get_local_memory_data_records(sdb).records_object
        if self.schema_translation:
            obj = fmt.apply_schema_translation(self.schema_translation, obj)
        return obj

    def get_or_create_local_stored_data_block(
        self, target_format: DataFormat
    ) -> StoredDataBlockMetadata:
        # TODO: this is a beast, tame it
        from snapflow.core.conversion import (
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
    local_storage: Storage,
    records: Any,
    nominal_schema: Schema = None,
    inferred_schema: Schema = None,
    created_by_node_key: str = None,
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    from snapflow.core.storage.storage import LocalMemoryStorageEngine

    logger.debug("CREATING DATA BLOCK")
    if isinstance(records, LocalMemoryDataRecords):
        ldr = records
        # Important: override nominal schema with LDR entry if it exists (the schema was EXPLICITLY ADDED by pipe on purpose)
        if ldr.nominal_schema is not None:
            nominal_schema = env.get_schema(ldr.nominal_schema)
    else:
        ldr = LocalMemoryDataRecords.from_records_object(records)
    if not nominal_schema:
        nominal_schema = env.get_schema("Any")
    if not inferred_schema:
        inferred_schema = ldr.data_format.infer_schema_from_records(ldr.records_object)
        env.add_new_generated_schema(inferred_schema)
    realized_schema = cast_to_realized_schema(env, inferred_schema, nominal_schema)
    # if is_any(nominal_schema):
    #     if not inferred_schema:
    #         inferred_schema = ldr.data_format.infer_schema_from_records(
    #             ldr.records_object
    #         )
    #         env.add_new_generated_schema(inferred_schema)
    #     realized_schema = inferred_schema
    # else:
    #     realized_schema = nominal_schema
    block = DataBlockMetadata(
        inferred_schema_key=inferred_schema.key if inferred_schema else None,
        nominal_schema_key=nominal_schema.key,
        realized_schema_key=realized_schema.key,
        record_count=ldr.record_count,
        created_by_node_key=created_by_node_key,
    )
    sdb = StoredDataBlockMetadata(  # type: ignore
        data_block=block,
        storage_url=local_storage.url,
        data_format=ldr.data_format,
    )
    env.session.add(block)
    env.session.add(sdb)
    env.session.flush([block, sdb])
    LocalMemoryStorageEngine(env, local_storage).store_local_memory_data_records(
        sdb, ldr
    )
    return block, sdb
