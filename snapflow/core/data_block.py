from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Iterator, Optional, Tuple, Type

from commonmodel.base import Schema, SchemaKey, SchemaTranslation
from dcp.data_copy.graph import StorageFormat
from dcp.data_format.base import DataFormat
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.data_format.handler import FormatHandler, get_handler_for_name
from dcp.storage.base import MemoryStorageClass, Storage
from dcp.storage.memory.engines.python import LOCAL_PYTHON_STORAGE
from dcp.utils.common import as_identifier, rand_str
from loguru import logger
from pandas import DataFrame
from snapflow.core.environment import Environment
from snapflow.core.metadata.orm import (
    BaseModel,
    DataFormatType,
    timestamp_increment_key,
)
from snapflow.utils.registry import ClassBasedEnumSqlalchemyType
from snapflow.utils.typing import T
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, select
from sqlalchemy.orm import RelationshipProperty, Session, relationship
from sqlalchemy.sql.schema import UniqueConstraint

if TYPE_CHECKING:
    from snapflow.core.execution.executable import ExecutionContext


def get_datablock_id() -> str:
    return timestamp_increment_key(prefix="db")


def get_stored_datablock_id() -> str:
    return timestamp_increment_key(prefix="sdb")


class DataBlockMetadata(BaseModel):  # , Generic[DT]):
    # NOTE on block ids: we generate them dynamically so we don't have to hit a central db for a sequence
    # BUT we MUST ensure they are monotonically ordered -- the logic of selecting the correct (most recent)
    # block relies on strict monotonic IDs in some scenarios
    id = Column(String(128), primary_key=True, default=get_datablock_id)
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
            record_count=self.record_count,
        )

    def inferred_schema(self, env: Environment) -> Optional[Schema]:
        return env.get_schema(self.inferred_schema_key)

    def nominal_schema(self, env: Environment) -> Optional[Schema]:
        return env.get_schema(self.nominal_schema_key)

    def realized_schema(self, env: Environment) -> Schema:
        return env.get_schema(self.realized_schema_key)

    def as_managed_data_block(
        self,
        env: Environment,
        schema_translation: Optional[SchemaTranslation] = None,
    ):
        mgr = DataBlockManager(env, self, schema_translation=schema_translation)
        return ManagedDataBlock(
            data_block_id=self.id,
            inferred_schema_key=self.inferred_schema_key,
            nominal_schema_key=self.nominal_schema_key,
            realized_schema_key=self.realized_schema_key,
            manager=mgr,
        )

    def compute_record_count(self) -> bool:
        for sdb in self.stored_data_blocks.all():
            cnt = sdb.record_count()
            if cnt is not None:
                self.record_count = cnt
                return True
        return False

    # def created_by(self: Session) -> Optional[str]:
    #     from snapflow.core.node import DataBlockLog
    #     from snapflow.core.node import DataFunctionLog
    #     from snapflow.core.node import Direction

    #     result = (
    #         env.md_api.execute(select(DataFunctionLog.node_key)
    #         .join(DataBlockLog)
    #         .filter(
    #             DataBlockLog.direction == Direction.OUTPUT,
    #             DataBlockLog.data_block_id == self.id,
    #         )
    #         .scalar_one_or_none()
    #     )
    #     if result:
    #         return result[0]
    #     return None


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
    # id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String(128), primary_key=True, default=get_stored_datablock_id)
    # id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=True)
    data_block_id = Column(
        String(128), ForeignKey(DataBlockMetadata.id), nullable=False
    )
    storage_url = Column(String(128), nullable=False)
    data_format: DataFormat = Column(DataFormatType, nullable=False)  # type: ignore
    aliases: RelationshipProperty = relationship(
        "Alias", backref="stored_data_block", lazy="dynamic"
    )
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

    def get_name_for_storage(self) -> str:
        if self.name:
            return self.name
        if self.id is None:
            raise Exception("ID not set yet")
        node_key = self.data_block.created_by_node_key or ""
        self.name = as_identifier(f"_{node_key[:30]}_{self.id}")
        return self.name

    def get_handler(self) -> FormatHandler:
        return get_handler_for_name(self.name, self.storage)()

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
        return Storage.from_url(self.storage_url)

    def get_storage_format(self) -> StorageFormat:
        return StorageFormat(self.storage.storage_engine, self.data_format)

    def exists(self) -> bool:
        return self.storage.get_api().exists(self.get_name_for_storage())

    def record_count(self) -> Optional[int]:
        if self.data_block.record_count is not None:
            return self.data_block.record_count
        return self.storage.get_api().record_count(self.get_name_for_storage())

    def get_alias(self, env: Environment) -> Optional[Alias]:
        return env.md_api.execute(
            select(Alias).filter(Alias.stored_data_block_id == self.id)
        ).scalar_one_or_none()

    def update_alias(self, env: Environment, new_alias: str):
        a: Alias = env.md_api.execute(
            select(Alias).filter(Alias.name == new_alias)
        ).scalar_one_or_none()
        if a is None:
            raise Exception("No alias to update")
        a.update_alias(env, new_alias)

    def create_alias(self, env: Environment, alias: str) -> Alias:
        # Create or update Alias
        a: Alias = env.md_api.execute(
            select(Alias).filter(Alias.name == alias)
        ).scalar_one_or_none()
        if a is None:
            # (not really a race condition here since alias is unique to node and node cannot
            #  run in parallel, for now at least)
            a = Alias(
                name=alias,
                data_block_id=self.data_block_id,
                stored_data_block_id=self.id,
            )
            env.md_api.add(a)
        else:
            a.data_block_id = self.data_block_id
            a.stored_data_block_id = self.id
        self.storage.get_api().create_alias(self.get_name_for_storage(), alias)
        return a


# event.listen(DataBlockMetadata, "before_update", immutability_update_listener)
# event.listen(StoredDataBlockMetadata, "before_update", add_persisting_sdb_listener)


class Alias(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128))
    data_block_id = Column(
        String(128), ForeignKey(DataBlockMetadata.id), nullable=False
    )
    stored_data_block_id = Column(
        String(128), ForeignKey(StoredDataBlockMetadata.id), nullable=False
    )
    # Hints
    data_block: "DataBlockMetadata"
    stored_data_block: "StoredDataBlockMetadata"

    __table_args__ = (UniqueConstraint("env_id", "name"),)

    def update_alias(self, env: Environment, new_alias: str):
        self.stored_data_block.storage.get_api().create_alias(
            self.stored_data_block.get_name_for_storage(), new_alias
        )
        self.stored_data_block.storage.get_api().remove_alias(self.name)
        self.name = new_alias
        env.md_api.add(self)


class DataBlockManager:
    def __init__(
        self,
        env: Environment,
        data_block: DataBlockMetadata,
        schema_translation: Optional[SchemaTranslation] = None,
    ):

        self.env = env

        self.data_block = data_block
        self.schema_translation = schema_translation

    def __str__(self):
        return f"DRM: {self.data_block}, Local: {self.env._local_python_storage}, rest: {self.env.storages}"

    # def get_runtime_storage(self) -> Optional[Storage]:
    #     if self.ctx.current_runtime is None:
    #         return None
    #     return self.ctx.current_runtime.as_storage()

    def inferred_schema(self) -> Optional[Schema]:
        if self.data_block.inferred_schema_key is None:
            return None
        return self.env.get_schema(self.data_block.inferred_schema_key)

    def nominal_schema(self) -> Optional[Schema]:
        if self.data_block.nominal_schema_key is None:
            return None
        return self.env.get_schema(self.data_block.nominal_schema_key)

    def realized_schema(self) -> Schema:
        if self.data_block.realized_schema_key is None:
            return None
        return self.env.get_schema(self.data_block.realized_schema_key)

    def as_dataframe(self) -> DataFrame:
        return self.as_format(DataFrameFormat)

    def as_records(self) -> Records:
        return self.as_format(RecordsFormat)

    def as_table(self, storage: Storage) -> str:
        return self.as_format(DatabaseTableFormat, storage)

    def as_format(self, fmt: DataFormat, storage: Storage = None) -> Any:
        with self.env.metadata_api.ensure_session() as sess:
            self.data_block = sess.merge(self.data_block)
            sdb = self.ensure_format(fmt, storage)
            return self.as_python_object(sdb)

    def ensure_format(self, fmt: DataFormat, target_storage: Storage = None) -> Any:
        from snapflow.core.storage import ensure_data_block_on_storage

        if fmt.natural_storage_class == MemoryStorageClass:
            # Ensure we are putting memory format in memory
            # if not target_storage.storage_engine.storage_class == PythonStorageClass:
            target_storage = self.env._local_python_storage
        assert target_storage is not None
        sdb = ensure_data_block_on_storage(
            self.env,
            block=self.data_block,
            storage=target_storage,
            fmt=fmt,
            eligible_storages=self.env.storages,
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

    # def get_or_create_local_stored_data_block(
    #     self, target_format: DataFormat
    # ) -> StoredDataBlockMetadata:
    #
    #         StorageFormat,
    #     )
    #

    #     cnt = (
    #         self.env.md_api.execute(select(StoredDataBlockMetadata)
    #         .filter(StoredDataBlockMetadata.data_block == self.data_block)
    #         .count()
    #     )
    #     logger.debug(f"{cnt} SDBs total")
    #     existing_sdbs = self.env.md_api.execute(select(StoredDataBlockMetadata).filter(
    #         StoredDataBlockMetadata.data_block == self.data_block,
    #         # DO NOT fetch memory SDBs that aren't of current runtime (since we can't get them!)
    #         # TODO: clean up memory SDBs when the memory goes away? Doesn't make sense to persist them really
    #         # Should be a separate in-memory lookup for memory SDBs, so they naturally expire?
    #         or_(
    #             ~StoredDataBlockMetadata.storage_url.startswith("memory:"),
    #             StoredDataBlockMetadata.storage_url
    #             == self.ctx.local_python_storage.url,
    #         ),
    #     )
    #     logger.debug(
    #         f"{existing_sdbs.count()} SDBs on-disk or in local memory (local: {self.ctx.local_python_storage.url})"
    #     )
    #     existing_sdbs.filter(
    #         StoredDataBlockMetadata.storage_url.in_(self.ctx.all_storages),
    #     )
    #     logger.debug(f"{existing_sdbs.count()} SDBs in eligible storages")
    #     target_storage_format = StorageFormat(
    #         self.ctx.local_python_storage.storage_engine, target_format
    #     )

    #     # Compute conversion costs
    #     eligible_conversion_paths = (
    #         []
    #     )  #: List[List[Tuple[ConversionCostLevel, Type[Converter]]]] = []
    #     existing_sdbs = list(existing_sdbs)
    #     for sdb in existing_sdbs:
    #         source_storage_format = StorageFormat(
    #             sdb.storage.storage_engine, sdb.data_format
    #         )
    #         if source_storage_format == target_storage_format:
    #             return sdb
    #         conversion_path = get_copy_path_for_sdb(
    #             sdb, target_storage_format, self.ctx.all_storages
    #         )
    #         if conversion_path is not None:
    #             eligible_conversion_paths.append(
    #                 (conversion_path.total_cost, conversion_path, sdb)
    #             )
    #     if not eligible_conversion_paths:
    #         raise NotImplementedError(
    #             f"No converter to {target_format} for existing StoredDataBlocks {existing_sdbs}. {self}"
    #         )
    #     cost, conversion_path, in_sdb = min(
    #         eligible_conversion_paths, key=lambda x: x[0]
    #     )
    #     return convert_sdb(self.ctx.env, in_sdb, conversion_path,
    #     storage)


# def create_data_block_from_records(
#     env: Environment,
#     local_storage: Storage,
#     records: Any,
#     nominal_schema: Schema = None,
#     inferred_schema: Schema = None,
#     created_by_node_key: str = None,
# ) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:

#     logger.debug("CREATING DATA BLOCK")
#     if isinstance(records, MemoryDataRecords):
#         dro = records
#         # Important: override nominal schema with DRO entry if it exists
#         if dro.nominal_schema is not None:
#             nominal_schema = env.get_schema(dro.nominal_schema)
#     else:
#         dro = as_records(records, schema=nominal_schema)
#     if not nominal_schema:
#         nominal_schema = env.get_schema("Any")
#     if not inferred_schema:
#         inferred_schema = dro.data_format.infer_schema_from_records(dro.records_object)
#         env.add_new_generated_schema(inferred_schema)
#     realized_schema = cast_to_realized_schema(env, inferred_schema, nominal_schema)
#     dro = dro.conform_to_schema(realized_schema)
#     block = DataBlockMetadata(
#         id=get_datablock_id(),
#         inferred_schema_key=inferred_schema.key if inferred_schema else None,
#         nominal_schema_key=nominal_schema.key,
#         realized_schema_key=realized_schema.key,
#         record_count=dro.record_count,
#         created_by_node_key=created_by_node_key,
#     )
#     sdb = StoredDataBlockMetadata(  # type: ignore
#         id=get_stored_datablock_id(),
#         data_block_id=block.id,
#         data_block=block,
#         storage_url=local_storage.url,
#         data_format=dro.data_format,
#     )
#     env.md_api.add(block)
#     env.md_api.add(sdb)
#     # env.md_api.flush([block, sdb])
#     local_storage.get_api().put(sdb.get_name_for_storage(), dro)
#     return block, sdb


# def create_data_block_from_sql(
#     env: Environment,
#     sql: str,
#     db_api: DatabaseStorageApi,
#     nominal_schema: Schema = None,
#     inferred_schema: Schema = None,
#     created_by_node_key: str = None,
# ) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
#     # TODO: we are special casing sql right now, but could create another DataFormat (SqlQueryFormat, non-storable).
#     #       but, not sure how well it fits paradigm (it's a fundamentally non-python operation, the only one for now --
#     #       if we had an R runtime or any other shell command, they would also be in this bucket)
#     #       fine here for now, but there is a generalization that might make the sql function less awkward (returning sdb)
#     logger.debug("CREATING DATA BLOCK from sql")
#     tmp_name = f"_tmp_{rand_str(10)}".lower()
#     sql = db_api.clean_sub_sql(sql)
#     create_sql = f"""
#     create table {tmp_name} as
#     select
#     *
#     from (
#     {sql}
#     ) as __sub
#     """
#     db_api.execute_sql(create_sql)
#     cnt = db_api.count(tmp_name)
#     if not nominal_schema:
#         nominal_schema = env.get_schema("Any")
#     if not inferred_schema:
#         inferred_schema = infer_schema_from_db_table(db_api, tmp_name)
#         env.add_new_generated_schema(inferred_schema)
#     realized_schema = cast_to_realized_schema(env, inferred_schema, nominal_schema)
#     block = DataBlockMetadata(
#         id=get_datablock_id(),
#         inferred_schema_key=inferred_schema.key if inferred_schema else None,
#         nominal_schema_key=nominal_schema.key,
#         realized_schema_key=realized_schema.key,
#         record_count=cnt,
#         created_by_node_key=created_by_node_key,
#     )
#     storage_url = db_api.url
#     sdb = StoredDataBlockMetadata(
#         id=get_stored_datablock_id(),
#         data_block_id=block.id,
#         data_block=block,
#         storage_url=storage_url,
#         data_format=DatabaseTableFormat,
#     )
#     env.md_api.add(block)
#     env.md_api.add(sdb)
#     # env.md_api.flush([block, sdb])
#     db_api.rename_table(tmp_name, sdb.get_name_for_storage())
#     return block, sdb
