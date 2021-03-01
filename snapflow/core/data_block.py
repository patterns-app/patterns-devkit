from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Iterator, Optional, Tuple, Type

from loguru import logger
from pandas import DataFrame
from snapflow.core.environment import Environment
from snapflow.core.metadata.orm import BaseModel, timestamp_increment_key
from snapflow.core.typing.casting import cast_to_realized_schema
from snapflow.core.typing.inference import infer_schema_from_db_table
from snapflow.schema import Schema, SchemaKey, SchemaLike, SchemaTranslation
from snapflow.storage.data_formats import (
    DataFormat,
    DataFrameFormat,
    get_data_format_of_object,
)
from snapflow.storage.data_formats.base import MemoryDataFormatBase
from snapflow.storage.data_formats.data_frame import DataFrameIteratorFormat
from snapflow.storage.data_formats.database_table import DatabaseTableFormat
from snapflow.storage.data_formats.database_table_ref import (
    DatabaseTableRef,
    DatabaseTableRefFormat,
)
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectIteratorFormat,
)
from snapflow.storage.data_formats.records import (
    Records,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.data_records import MemoryDataRecords, as_records
from snapflow.storage.db.api import DatabaseStorageApi
from snapflow.storage.storage import PythonStorageClass
from snapflow.utils.common import as_identifier, rand_str
from snapflow.utils.registry import ClassBasedEnumSqlalchemyType
from snapflow.utils.typing import T
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, event, or_
from sqlalchemy.orm import RelationshipProperty, Session, relationship

if TYPE_CHECKING:
    from snapflow.core.storage import ensure_data_block_on_storage
    from snapflow.core.storage import convert_sdb, get_copy_path_for_sdb
    from snapflow.storage.data_copy.base import StorageFormat
    from snapflow.core.execution import RunContext
    from snapflow.storage.storage import (
        Storage,
        LocalPythonStorageEngine,
    )


def get_datablock_id() -> str:
    return timestamp_increment_key()


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

    def inferred_schema(self, env: Environment, sess: Session) -> Optional[Schema]:
        return env.get_schema(self.inferred_schema_key, sess)

    def nominal_schema(self, env: Environment, sess: Session) -> Optional[Schema]:
        return env.get_schema(self.nominal_schema_key, sess)

    def realized_schema(self, env: Environment, sess: Session) -> Schema:
        return env.get_schema(self.realized_schema_key, sess)

    def as_managed_data_block(
        self,
        ctx: RunContext,
        sess: Session,
        schema_translation: Optional[SchemaTranslation] = None,
    ):
        mgr = DataBlockManager(ctx, sess, self, schema_translation=schema_translation)
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

    # def created_by(self, sess: Session) -> Optional[str]:
    #     from snapflow.core.node import DataBlockLog
    #     from snapflow.core.node import SnapLog
    #     from snapflow.core.node import Direction

    #     result = (
    #         sess.query(SnapLog.node_key)
    #         .join(DataBlockLog)
    #         .filter(
    #             DataBlockLog.direction == Direction.OUTPUT,
    #             DataBlockLog.data_block_id == self.id,
    #         )
    #         .first()
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

    def as_dataframe_iterator(self) -> DataFrame:
        return self.manager.as_format(DataFrameIteratorFormat)

    def as_records(self) -> Records:
        return self.manager.as_records()

    def as_records_iterator(self) -> Records:
        return self.manager.as_format(RecordsIteratorFormat)

    def as_table(self) -> DatabaseTableRef:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    def as_table_stmt(self) -> str:
        return self.as_table().get_table_stmt()

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
    id = Column(String(128), primary_key=True, default=get_datablock_id)
    # id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=True)
    data_block_id = Column(
        String(128), ForeignKey(DataBlockMetadata.id), nullable=False
    )
    storage_url = Column(String(128), nullable=False)
    data_format: DataFormat = Column(ClassBasedEnumSqlalchemyType, nullable=False)  # type: ignore
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

    def inferred_schema(self, env: Environment, sess: Session) -> Optional[Schema]:
        if self.data_block.inferred_schema_key is None:
            return None
        return env.get_schema(self.data_block.inferred_schema_key, sess)

    def nominal_schema(self, env: Environment, sess: Session) -> Optional[Schema]:
        if self.data_block.nominal_schema_key is None:
            return None
        return env.get_schema(self.data_block.nominal_schema_key, sess)

    def realized_schema(self, env: Environment, sess: Session) -> Schema:
        if self.data_block.realized_schema_key is None:
            return None
        return env.get_schema(self.data_block.realized_schema_key, sess)

    @property
    def storage(self) -> Storage:
        from snapflow.storage.storage import Storage

        return Storage.from_url(self.storage_url)

    def get_name(self) -> str:
        if self.name:
            return self.name
        if self.data_block_id is None or self.id is None:
            raise Exception("Id not set yet")
        node_key = self.data_block.created_by_node_key or ""
        self.name = as_identifier(f"_{node_key[:40]}_{self.id}")
        return self.name

    def get_storage_format(self) -> StorageFormat:
        return StorageFormat(self.storage.storage_engine, self.data_format)

    def exists(self) -> bool:
        return self.storage.get_api().exists(self.get_name())

    def record_count(self) -> Optional[int]:
        if self.data_block.record_count is not None:
            return self.data_block.record_count
        return self.storage.get_api().record_count(self.get_name())

    def get_alias(self, sess: Session) -> Optional[Alias]:
        return sess.query(Alias).filter(Alias.stored_data_block_id == self.id).first()

    def create_alias(self, sess: Session, alias: str) -> Alias:
        # Create or update Alias
        a: Alias = sess.query(Alias).filter(Alias.alias == alias).first()
        if a is None:
            # (not really a race condition here since alias is unique to node and node cannot
            #  run in parallel, for now at least)
            a = Alias(
                alias=alias,
                data_block_id=self.data_block_id,
                stored_data_block_id=self.id,
            )
            sess.add(a)
        else:
            a.data_block_id = self.data_block_id
            a.stored_data_block_id = self.id
        self.storage.get_api().create_alias(self.get_name(), alias)
        return a


# event.listen(DataBlockMetadata, "before_update", immutability_update_listener)
# event.listen(StoredDataBlockMetadata, "before_update", add_persisting_sdb_listener)


class Alias(BaseModel):
    alias = Column(String(128), primary_key=True)
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
        ctx: RunContext,
        sess: Session,
        data_block: DataBlockMetadata,
        schema_translation: Optional[SchemaTranslation] = None,
    ):

        self.ctx = ctx
        self.sess = sess
        self.data_block = data_block
        self.schema_translation = schema_translation

    def __str__(self):
        return f"DRM: {self.data_block}, Local: {self.ctx.local_python_storage}, rest: {self.ctx.storages}"

    def get_runtime_storage(self) -> Optional[Storage]:
        if self.ctx.current_runtime is None:
            return None
        return self.ctx.current_runtime.as_storage()

    def inferred_schema(self) -> Optional[Schema]:
        if self.data_block.inferred_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.inferred_schema_key, self.sess)

    def nominal_schema(self) -> Optional[Schema]:
        if self.data_block.nominal_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.nominal_schema_key, self.sess)

    def realized_schema(self) -> Schema:
        if self.data_block.realized_schema_key is None:
            return None
        return self.ctx.env.get_schema(self.data_block.realized_schema_key, self.sess)

    def as_dataframe(self) -> DataFrame:
        return self.as_format(DataFrameFormat)

    def as_dataframe_iterator(self) -> Iterator[DataFrame]:
        return self.as_format(DataFrameIteratorFormat)

    def as_records(self) -> Records:
        return self.as_format(RecordsFormat)

    def as_records_iterator(self) -> Iterator[Records]:
        return self.as_format(RecordsIteratorFormat)

    def as_table(self) -> DatabaseTableRef:
        return self.as_format(DatabaseTableFormat)

    def as_format(self, fmt: DataFormat) -> Any:
        sdb = self.ensure_format(fmt)
        return self.as_python_object(sdb)

    def ensure_format(self, fmt: DataFormat) -> Any:
        from snapflow.core.storage import ensure_data_block_on_storage

        target_storage = self.get_runtime_storage()
        if fmt.is_python_format():
            # Ensure we are putting memory format in memory
            # if not target_storage.storage_engine.storage_class == PythonStorageClass:
            target_storage = self.ctx.local_python_storage
        assert target_storage is not None
        sdb = ensure_data_block_on_storage(
            self.ctx.env,
            self.sess,
            block=self.data_block,
            storage=target_storage,
            fmt=fmt,
            eligible_storages=self.ctx.storages,
        )
        return sdb

    def as_python_object(self, sdb: StoredDataBlockMetadata) -> Any:
        if sdb.data_format.is_python_format():
            obj = sdb.storage.get_api().get(sdb.get_name()).records_object
        else:
            if sdb.data_format == DatabaseTableFormat:
                obj = DatabaseTableRef(sdb.get_name(), storage_url=sdb.storage.url)
            else:
                # TODO: what is general solution to this? if we do DataFormat.as_python_object(sdb) then
                #       we have formats depending on StoredDataBlocks again, and no seperation of concerns
                #       BUT what other option is there? Need knowledge of storage url and name to make useful "pointer" object
                raise NotImplementedError(
                    f"Don't know how to bring '{sdb.data_format}' into python'"
                )
        if self.schema_translation:
            obj = sdb.data_format.apply_schema_translation(self.schema_translation, obj)
        return obj

    def has_format(self, fmt: DataFormat) -> bool:
        return (
            self.sess.query(StoredDataBlockMetadata)
            .filter(StoredDataBlockMetadata.data_block == self.data_block)
            .filter(StoredDataBlockMetadata.data_format == fmt)
            .count()
        ) > 0

    # def get_or_create_local_stored_data_block(
    #     self, target_format: DataFormat
    # ) -> StoredDataBlockMetadata:
    #     from snapflow.storage.data_copy.base import (
    #         StorageFormat,
    #     )
    #     from snapflow.core.storage import convert_sdb, get_copy_path_for_sdb

    #     cnt = (
    #         self.sess.query(StoredDataBlockMetadata)
    #         .filter(StoredDataBlockMetadata.data_block == self.data_block)
    #         .count()
    #     )
    #     logger.debug(f"{cnt} SDBs total")
    #     existing_sdbs = self.sess.query(StoredDataBlockMetadata).filter(
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
    #     return convert_sdb(self.ctx.env, self.sess, in_sdb, conversion_path,
    #     storage)


def create_data_block_from_records(
    env: Environment,
    sess: Session,
    local_storage: Storage,
    records: Any,
    nominal_schema: Schema = None,
    inferred_schema: Schema = None,
    created_by_node_key: str = None,
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    from snapflow.storage.storage import LocalPythonStorageEngine

    logger.debug("CREATING DATA BLOCK")
    if isinstance(records, MemoryDataRecords):
        dro = records
        # Important: override nominal schema with DRO entry if it exists
        if dro.nominal_schema is not None:
            nominal_schema = env.get_schema(dro.nominal_schema, sess)
    else:
        dro = as_records(records, schema=nominal_schema)
    if not nominal_schema:
        nominal_schema = env.get_schema("Any", sess)
    if not inferred_schema:
        inferred_schema = dro.data_format.infer_schema_from_records(dro.records_object)
        env.add_new_generated_schema(inferred_schema, sess)
    realized_schema = cast_to_realized_schema(
        env, sess, inferred_schema, nominal_schema
    )
    dro = dro.conform_to_schema(realized_schema)
    block = DataBlockMetadata(
        id=get_datablock_id(),
        inferred_schema_key=inferred_schema.key if inferred_schema else None,
        nominal_schema_key=nominal_schema.key,
        realized_schema_key=realized_schema.key,
        record_count=dro.record_count,
        created_by_node_key=created_by_node_key,
    )
    sdb = StoredDataBlockMetadata(  # type: ignore
        id=get_datablock_id(),
        data_block_id=block.id,
        data_block=block,
        storage_url=local_storage.url,
        data_format=dro.data_format,
    )
    sess.add(block)
    sess.add(sdb)
    # sess.flush([block, sdb])
    local_storage.get_api().put(sdb.get_name(), dro)
    return block, sdb


def create_data_block_from_sql(
    env: Environment,
    sql: str,
    sess: Session,
    db_api: DatabaseStorageApi,
    nominal_schema: Schema = None,
    inferred_schema: Schema = None,
    created_by_node_key: str = None,
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    # TODO: we are special casing sql right now, but could create another DataFormat (SqlQueryFormat, non-storable).
    #       but, not sure how well it fits paradigm (it's a fundamentally non-python operation, the only one for now --
    #       if we had an R runtime or any other shell command, they would also be in this bucket)
    #       fine here for now, but there is a generalization that might make the sql snap less awkward (returning sdb)
    logger.debug("CREATING DATA BLOCK from sql")
    tmp_name = f"_tmp_{rand_str(10)}".lower()
    sql = db_api.clean_sub_sql(sql)
    create_sql = f"""
    create table {tmp_name} as
    select
    *
    from (
    {sql}
    ) as __sub
    """
    db_api.execute_sql(create_sql)
    cnt = db_api.count(tmp_name)
    if not nominal_schema:
        nominal_schema = env.get_schema("Any", sess)
    if not inferred_schema:
        inferred_schema = infer_schema_from_db_table(db_api, tmp_name)
        env.add_new_generated_schema(inferred_schema, sess)
    realized_schema = cast_to_realized_schema(
        env, sess, inferred_schema, nominal_schema
    )
    block = DataBlockMetadata(
        id=get_datablock_id(),
        inferred_schema_key=inferred_schema.key if inferred_schema else None,
        nominal_schema_key=nominal_schema.key,
        realized_schema_key=realized_schema.key,
        record_count=cnt,
        created_by_node_key=created_by_node_key,
    )
    storage_url = db_api.url
    sdb = StoredDataBlockMetadata(
        id=get_datablock_id(),
        data_block_id=block.id,
        data_block=block,
        storage_url=storage_url,
        data_format=DatabaseTableFormat,
    )
    sess.add(block)
    sess.add(sdb)
    # sess.flush([block, sdb])
    db_api.rename_table(tmp_name, sdb.get_name())
    return block, sdb
