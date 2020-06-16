from __future__ import annotations

from collections import abc
from dataclasses import dataclass
from itertools import _tee, tee
from typing import TYPE_CHECKING, Any, Generic, Optional, Tuple

import sqlalchemy as sa
from pandas import DataFrame
from sqlalchemy import Boolean, Column, ForeignKey, String, event, or_
from sqlalchemy.orm import RelationshipProperty, Session, relationship

from basis.core.data_format import (
    DatabaseTable,
    DataFormat,
    RecordsList,
    get_data_format_of_object,
    get_records_list_sample,
)
from basis.core.environment import Environment
from basis.core.metadata.listeners import immutability_update_listener
from basis.core.metadata.orm import BaseModel, timestamp_rand_key
from basis.core.typing.inference import infer_otype_from_records_list
from basis.core.typing.object_type import ObjectType, ObjectTypeUri, is_any
from basis.utils.typing import T

if TYPE_CHECKING:
    from basis.core.conversion import (
        StorageFormat,
        get_conversion_path_for_sdb,
        convert_sdb,
    )
    from basis.core.runnable import ExecutionContext
    from basis.core.storage.storage import (
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
        if record_count is None:
            record_count = data_format.get_manager().get_record_count(obj)
        return LocalMemoryDataRecords(
            data_format=data_format, records_object=obj, record_count=record_count
        )

    def copy(self) -> LocalMemoryDataRecords:
        records_object = self.data_format.get_manager().copy_records(
            self.records_object
        )
        return LocalMemoryDataRecords(
            data_format=self.data_format,
            record_count=self.record_count,
            records_object=records_object,
        )

    def validate_and_conform_otype(self, expected_otype: ObjectType):
        # TODO: idea here is to make sure local records look like what we expect. part of larger project on ObjectType
        #   validation
        # if self.data_format == DataFormat.DATAFRAME:
        #     from basis.core.pandas import coerce_dataframe_to_otype
        #
        #     printd(f"Before validation")
        #     printd(f"-----------------")
        #     printd(self.records_object)
        #     coerce_dataframe_to_otype(self.records_object, otype)
        #     printd(f"After validation")
        #     printd(f"-----------------")
        #     printd(self.records_object)
        # # TODO: other formats
        pass

    @property
    def record_count_display(self):
        return self.record_count if self.record_count is not None else "Unknown"


class DataBlockMetadata(BaseModel):  # , Generic[DT]):
    id = Column(String, primary_key=True, default=timestamp_rand_key)
    # name = Column(String) ????
    # otype_uri: ObjectTypeUri = Column(String, nullable=False)  # type: ignore
    expected_otype_uri: ObjectTypeUri = Column(String, nullable=True)  # type: ignore
    realized_otype_uri: ObjectTypeUri = Column(String, nullable=True)  # type: ignore
    # metadata_is_set = Column(Boolean, default=False)
    # otype_is_validated = Column(Boolean, default=False) # TODO
    # references_are_resolved = Column(Boolean, default=False)
    # is_dataset = Column(Boolean, default=False)
    # record_count = Column(Integer, nullable=True) # TODO: nahhh, this belongs on some DataBlockMetadata table (optional, computed lazily)
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
            expected_otype_uri=self.expected_otype_uri,
            realized_otype_uri=self.realized_otype_uri,
        )

    @property
    def most_real_otype_uri(self) -> ObjectTypeUri:
        return self.realized_otype_uri or self.expected_otype_uri

    @property
    def most_abstract_otype_uri(self) -> ObjectTypeUri:
        return self.expected_otype_uri or self.realized_otype_uri

    def as_managed_data_block(self, ctx: ExecutionContext):
        mgr = DataBlockManager(ctx, self,)
        return ManagedDataBlock(
            data_block_id=self.id,
            expected_otype_uri=self.expected_otype_uri,
            realized_otype_uri=self.realized_otype_uri,
            manager=mgr,
        )

    def created_by(self, sess: Session) -> Optional[str]:
        from basis.core.function_node import DataBlockLog
        from basis.core.function_node import DataFunctionLog
        from basis.core.function_node import Direction

        result = (
            sess.query(DataFunctionLog.function_node_name)
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
    expected_otype_uri: ObjectTypeUri
    realized_otype_uri: ObjectTypeUri
    # otype_is_validated: bool
    manager: DataBlockManager

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_records_list(self) -> RecordsList:
        return self.manager.as_records_list()

    def as_table(self) -> DatabaseTable:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    @property
    def expected_otype(self) -> ObjectType:
        return self.manager.get_expected_otype()

    @property
    def realized_otype(self) -> ObjectType:
        return self.manager.get_realized_otype()


DataBlock = ManagedDataBlock


class StoredDataBlockMetadata(BaseModel):
    id = Column(String, primary_key=True, default=timestamp_rand_key)
    data_block_id = Column(String, ForeignKey(DataBlockMetadata.id), nullable=False)
    storage_url = Column(String, nullable=False)
    data_format: DataFormat = Column(sa.Enum(DataFormat), nullable=False)  # type: ignore
    # is_ephemeral = Column(Boolean, default=False) # TODO
    # data_records_object: Optional[Any]  # Union[DataFrame, FilePointer, List[Dict]]
    # Hints
    data_block: "DataBlockMetadata"

    def __repr__(self):
        return self._repr(
            id=self.id,
            data_block=self.data_block,
            data_format=self.data_format,
            storage_url=self.storage_url,
        )

    def get_realized_otype(self, env: Environment) -> ObjectType:
        if self.data_block.realized_otype_uri is None:
            return None
        return env.get_otype(self.data_block.realized_otype_uri)

    def get_expected_otype(self, env: Environment) -> ObjectType:
        if self.data_block.expected_otype_uri is None:
            return None
        return env.get_otype(self.data_block.expected_otype_uri)

    @property
    def storage(self) -> Storage:
        from basis.core.storage.storage import Storage

        return Storage.from_url(self.storage_url)

    def get_name(self, env: Environment) -> str:
        if self.data_block_id is None or self.id is None:
            raise Exception(
                "Trying to get SDR name, but IDs not set yet"
            )  # TODO, better exceptions
        otype = env.get_otype(self.data_block.most_real_otype_uri)
        return f"_{otype.get_identifier()[:40]}_{self.id}"  # TODO: max table name lengths in other engines? (63 in postgres)

    def get_storage_format(self) -> StorageFormat:
        return StorageFormat(self.storage.storage_type, self.data_format)

    def exists(self, env: Environment) -> bool:
        return self.storage.get_manager(env).exists(self)

    def record_count(self, env: Environment) -> Optional[int]:
        # TODO: this is really a property of a DR, but can only be computed by a SDR?
        return self.storage.get_manager(env).record_count(self)


event.listen(DataBlockMetadata, "before_update", immutability_update_listener)
event.listen(StoredDataBlockMetadata, "before_update", immutability_update_listener)


class DataSetMetadata(BaseModel):
    id = Column(String, primary_key=True, default=timestamp_rand_key)
    name = Column(String, nullable=False)
    expected_otype_uri: ObjectTypeUri = Column(String, nullable=True)  # type: ignore
    realized_otype_uri: ObjectTypeUri = Column(String, nullable=False)  # type: ignore
    data_block_id = Column(String, ForeignKey(DataBlockMetadata.id), nullable=False)
    # Hints
    data_block: "DataBlockMetadata"

    # __mapper_args__ = {
    #     "polymorphic_identity": True,
    # }

    def __repr__(self):
        return self._repr(
            id=self.id,
            name=self.name,
            expected_otype_uri=self.expected_otype_uri,
            realized_otype_uri=self.realized_otype_uri,
            data_block=self.data_block,
        )

    def as_managed_data_block(self, ctx: ExecutionContext):
        mgr = DataBlockManager(ctx, self.data_block)
        return ManagedDataSet(
            data_set_id=self.id,
            data_set_name=self.name,
            data_block_id=self.data_block_id,
            expected_otype_uri=self.expected_otype_uri,
            realized_otype_uri=self.realized_otype_uri,
            manager=mgr,
        )


@dataclass(frozen=True)
class ManagedDataSet(Generic[T]):
    data_set_id: str
    data_set_name: str
    data_block_id: str
    expected_otype_uri: ObjectTypeUri
    realized_otype_uri: ObjectTypeUri
    # otype_is_validated: bool
    manager: DataBlockManager

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_records_list(self) -> RecordsList:
        return self.manager.as_records_list()

    def as_table(self) -> DatabaseTable:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    @property
    def expected_otype(self) -> ObjectType:
        return self.manager.get_expected_otype()

    @property
    def realized_otype(self) -> ObjectType:
        return self.manager.get_realized_otype()


DataSet = ManagedDataSet


class DataBlockManager:
    def __init__(
        self, ctx: ExecutionContext, data_block: DataBlockMetadata,
    ):

        self.ctx = ctx
        self.data_block = data_block

    def __str__(self):
        return f"DRM: {self.data_block}, Local: {self.ctx.local_memory_storage}, rest: {self.ctx.storages}"

    def get_realized_otype(self) -> Optional[ObjectType]:
        if self.data_block.realized_otype_uri is None:
            return None
        return self.ctx.env.get_otype(self.data_block.realized_otype_uri)

    def get_expected_otype(self) -> Optional[ObjectType]:
        if self.data_block.expected_otype_uri is None:
            return None
        return self.ctx.env.get_otype(self.data_block.expected_otype_uri)

    def as_dataframe(self) -> DataFrame:
        return self.as_format(DataFormat.DATAFRAME)

    def as_records_list(self) -> RecordsList:
        return self.as_format(DataFormat.RECORDS_LIST)

    def as_table(self) -> DatabaseTable:
        return self.as_format(DataFormat.DATABASE_TABLE_REF)

    def is_valid_storage_format(self, fmt: StorageFormat) -> bool:
        return True  # TODO

    def as_format(self, fmt: DataFormat) -> Any:
        from basis.core.storage.storage import LocalMemoryStorageEngine

        sdb = self.get_or_create_local_stored_data_block(fmt)
        local_memory_storage = LocalMemoryStorageEngine(
            self.ctx.env, self.ctx.local_memory_storage
        )
        return local_memory_storage.get_local_memory_data_records(sdb).records_object

    def get_or_create_local_stored_data_block(
        self, target_format: DataFormat
    ) -> StoredDataBlockMetadata:
        from basis.core.conversion import (
            StorageFormat,
            get_conversion_path_for_sdb,
            convert_sdb,
        )

        existing_sdbs = self.ctx.metadata_session.query(StoredDataBlockMetadata).filter(
            StoredDataBlockMetadata.data_block == self.data_block,
            # TODO: why do we persist memory SDRs at all? Need more robust solution to this. Either separate class or some flag?
            #   Nice to be able to query all together via orm... hmmmm
            # DO NOT fetch memory SDRs that aren't of current runtime (since we can't get them!
            or_(
                ~StoredDataBlockMetadata.storage_url.startswith(
                    "memory:"
                ),  # TODO: drawback of just having url for StorageResource instead of proper metadata object
                StoredDataBlockMetadata.storage_url
                == self.ctx.local_memory_storage.url,
            ),
        )
        existing_sdbs.filter(
            StoredDataBlockMetadata.storage_url.in_(self.ctx.all_storages),
        )
        target_storage_format = StorageFormat(
            self.ctx.local_memory_storage.storage_type, target_format
        )
        if not self.is_valid_storage_format(target_storage_format):
            raise  # TODO

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


# class DataBlockFactory:
#     def __init__(self, env: Environment):
#         self.env = env
#
#     def create_data_block_from_records(self, records: Any) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:


def create_data_block_from_records(
    env: Environment,
    sess: Session,
    local_storage: Storage,
    records: Any,
    expected_otype: ObjectType = None,
    realized_otype: ObjectType = None,
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    from basis.core.storage.storage import LocalMemoryStorageEngine

    if not expected_otype:
        expected_otype = env.get_otype("Any")
    expected_otype_uri = expected_otype.uri
    if not realized_otype:
        if is_any(expected_otype):
            dl = get_records_list_sample(records)
            realized_otype = infer_otype_from_records_list(dl)
            env.add_new_otype(realized_otype)
        else:
            realized_otype = expected_otype
    realized_otype_uri = realized_otype.uri
    ldr = LocalMemoryDataRecords.from_records_object(records)
    block = DataBlockMetadata(
        expected_otype_uri=expected_otype_uri, realized_otype_uri=realized_otype_uri
    )
    sdb = StoredDataBlockMetadata(  # type: ignore
        data_block=block, storage_url=local_storage.url, data_format=ldr.data_format,
    )
    sess.add(block)
    sess.add(sdb)
    # TODO: Don't understand merge still
    block = sess.merge(block)
    sdb = sess.merge(sdb)
    LocalMemoryStorageEngine(env, local_storage).store_local_memory_data_records(
        sdb, ldr
    )
    return block, sdb
