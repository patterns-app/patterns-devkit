from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

import sqlalchemy as sa
from pandas import DataFrame
from sqlalchemy import Boolean, Column, ForeignKey, String, event, or_
from sqlalchemy.orm import RelationshipProperty, Session, object_session, relationship

from basis.core.data_format import (
    DatabaseTable,
    DataFormat,
    DictList,
    get_data_format_of_object,
)
from basis.core.environment import Environment
from basis.core.metadata.listeners import immutability_update_listener
from basis.core.metadata.orm import BaseModel, timestamp_rand_key
from basis.core.object_type import ObjectType, ObjectTypeUri

if TYPE_CHECKING:
    from basis.core.conversion import (
        StorageFormat,
        get_conversion_path_for_sdr,
        convert_sdr,
    )
    from basis.core.runnable import ExecutionContext
    from basis.core.storage import (
        Storage,
        LocalMemoryStorageEngine,
    )


@dataclass(frozen=True)
class LocalMemoryDataRecords:
    data_format: DataFormat
    records_object: Any  # TODO: only eligible types: DataFrame, DictList, table name (str)
    record_count: Optional[int] = None

    @classmethod
    def from_records_object(cls, obj: Any) -> LocalMemoryDataRecords:
        fmt = get_data_format_of_object(obj)
        if fmt is None:
            raise NotImplementedError(fmt)
        cnt = fmt.get_manager().get_record_count(obj)
        return LocalMemoryDataRecords(
            data_format=fmt, records_object=obj, record_count=cnt
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

    def validate_and_conform_otype(self, otype: ObjectType):
        # TODO: idea here is to make sure local records look like what we expect. part of larger project on ObjectType
        #   validation
        # if self.data_format == DataFormat.DATAFRAME:
        #     from basis.core.pandas_utils import coerce_dataframe_to_otype
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


class DataResourceMetadata(BaseModel):  # , Generic[DT]):
    id = Column(String, primary_key=True, default=timestamp_rand_key)
    # name = Column(String) ????
    otype_uri: ObjectTypeUri = Column(String, nullable=False)  # type: ignore
    # metadata_is_set = Column(Boolean, default=False)
    # otype_is_validated = Column(Boolean, default=False) # TODO
    # references_are_resolved = Column(Boolean, default=False)
    # is_dataset = Column(Boolean, default=False)
    # record_count = Column(Integer, nullable=True) # TODO: nahhh, this belongs on some DataResourceMetadata table (optional, computed lazily)
    # Other metadata? created_by_job? last_processed_at?
    deleted = Column(Boolean, default=False)
    data_sets: RelationshipProperty = relationship(
        "DataSetMetadata", backref="data_resource"
    )
    stored_data_resources: RelationshipProperty = relationship(
        "StoredDataResourceMetadata", backref="data_resource", lazy="dynamic"
    )
    data_resource_logs: RelationshipProperty = relationship(
        "DataResourceLog", backref="data_resource"
    )

    def __repr__(self):
        return self._repr(id=self.id, otype_uri=self.otype_uri)

    def as_managed_data_resource(self, ctx: ExecutionContext):
        mgr = DataResourceManager(ctx, self,)
        return ManagedDataResource(
            data_resource_id=self.id, otype_uri=self.otype_uri, manager=mgr
        )

    def created_by(self, sess: Session) -> Optional[str]:
        from basis.core.data_function import DataFunctionLog, DataResourceLog
        from basis.core.data_function import Direction

        result = (
            sess.query(DataFunctionLog.configured_data_function_key)
            .join(DataResourceLog)
            .filter(
                DataResourceLog.direction == Direction.OUTPUT,
                DataResourceLog.data_resource_id == self.id,
            )
            .first()
        )
        if result:
            return result[0]
        return None


@dataclass(frozen=True)
class ManagedDataResource:
    data_resource_id: str
    otype_uri: ObjectTypeUri
    # otype_is_validated: bool
    manager: DataResourceManager

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_dictlist(self) -> DictList:
        return self.manager.as_dictlist()

    def as_table(self) -> DatabaseTable:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    @property
    def otype(self) -> ObjectType:
        return self.manager.get_otype()


DataResource = ManagedDataResource


class StoredDataResourceMetadata(BaseModel):
    id = Column(String, primary_key=True, default=timestamp_rand_key)
    data_resource_id = Column(
        String, ForeignKey(DataResourceMetadata.id), nullable=False
    )
    storage_url = Column(String, nullable=False)
    data_format: DataFormat = Column(sa.Enum(DataFormat), nullable=False)  # type: ignore
    # is_ephemeral = Column(Boolean, default=False) # TODO
    # data_records_object: Optional[Any]  # Union[DataFrame, FilePointer, List[Dict]]
    # Hints
    data_resource: "DataResourceMetadata"

    def __repr__(self):
        return self._repr(
            id=self.id,
            data_resource=self.data_resource,
            data_format=self.data_format,
            storage_url=self.storage_url,
        )

    def get_otype(self, env: Environment) -> ObjectType:
        return env.get_otype(self.data_resource.otype_uri)

    @property
    def storage(self) -> Storage:
        from basis.core.storage import Storage

        return Storage.from_url(self.storage_url)

    def get_name(self, env: Environment) -> str:
        if self.data_resource_id is None or self.id is None:
            raise Exception(
                "Trying to get SDR name, but IDs not set yet"
            )  # TODO, better exceptions
        otype = env.get_otype(self.data_resource.otype_uri)
        return f"_{otype.get_identifier()[:40]}_{self.id}"  # TODO: max table name lengths in other engines? (63 in postgres)

    def get_storage_format(self) -> StorageFormat:
        return StorageFormat(self.storage.storage_type, self.data_format)

    def exists(self, env: Environment) -> bool:
        return self.storage.get_manager(env).exists(self)

    def record_count(self, env: Environment) -> Optional[int]:
        # TODO: this is really a property of a DR, but can only be computed by a SDR?
        return self.storage.get_manager(env).record_count(self)


event.listen(DataResourceMetadata, "before_update", immutability_update_listener)
event.listen(StoredDataResourceMetadata, "before_update", immutability_update_listener)


class DataSetMetadata(BaseModel):
    id = Column(String, primary_key=True, default=timestamp_rand_key)
    key = Column(String, nullable=False)
    otype_uri: ObjectTypeUri = Column(String, nullable=False)  # type: ignore
    data_resource_id = Column(
        String, ForeignKey(DataResourceMetadata.id), nullable=False
    )
    # Hints
    data_resource: "DataResourceMetadata"

    # __mapper_args__ = {
    #     "polymorphic_identity": True,
    # }

    def __repr__(self):
        return self._repr(
            id=self.id,
            name=self.key,
            otype_uri=self.otype_uri,
            data_resource=self.data_resource,
        )

    def as_managed_data_resource(self, ctx: ExecutionContext):
        mgr = DataResourceManager(ctx, self.data_resource)
        return ManagedDataSet(
            data_set_id=self.id,
            data_set_key=self.key,
            data_resource_id=self.data_resource_id,
            otype_uri=self.otype_uri,
            manager=mgr,
        )


@dataclass(frozen=True)
class ManagedDataSet:
    data_set_id: str
    data_set_key: str
    data_resource_id: str
    otype_uri: ObjectTypeUri
    # otype_is_validated: bool
    manager: DataResourceManager

    def as_dataframe(self) -> DataFrame:
        return self.manager.as_dataframe()

    def as_dictlist(self) -> DictList:
        return self.manager.as_dictlist()

    def as_table(self) -> DatabaseTable:
        return self.manager.as_table()

    def as_format(self, fmt: DataFormat) -> Any:
        return self.manager.as_format(fmt)

    @property
    def otype(self) -> ObjectType:
        return self.manager.get_otype()


DataSet = ManagedDataSet


class DataResourceManager:
    def __init__(
        self, ctx: ExecutionContext, data_resource: DataResourceMetadata,
    ):

        self.ctx = ctx
        self.data_resource = data_resource

    def __str__(self):
        return f"DRM: {self.data_resource}, Local: {self.ctx.local_memory_storage}, rest: {self.ctx.storages}"

    def get_otype(self) -> ObjectType:
        return self.ctx.env.get_otype(self.data_resource.otype_uri)

    def as_dataframe(self) -> DataFrame:
        return self.as_format(DataFormat.DATAFRAME)

    def as_dictlist(self) -> DictList:
        return self.as_format(DataFormat.DICT_LIST)

    def as_table(self) -> DatabaseTable:
        return self.as_format(DataFormat.DATABASE_TABLE_REF)

    def is_valid_storage_format(self, fmt: StorageFormat) -> bool:
        return True  # TODO

    def as_format(self, fmt: DataFormat) -> Any:
        from basis.core.storage import LocalMemoryStorageEngine

        sdr = self.get_or_create_local_stored_data_resource(fmt)
        local_memory_storage = LocalMemoryStorageEngine(
            self.ctx.env, self.ctx.local_memory_storage
        )
        return local_memory_storage.get_local_memory_data_records(sdr).records_object

    def get_or_create_local_stored_data_resource(
        self, target_format: DataFormat
    ) -> StoredDataResourceMetadata:
        from basis.core.conversion import StorageFormat

        existing_sdrs = self.ctx.metadata_session.query(
            StoredDataResourceMetadata
        ).filter(
            StoredDataResourceMetadata.data_resource == self.data_resource,
            # TODO: why do we persist memory SDRs at all? Need more robust solution to this. Either separate class or some flag?
            #   Nice to be able to query all together via orm... hmmmm
            # DO NOT fetch memory SDRs that aren't of current runtime (since we can't get them!
            or_(
                ~StoredDataResourceMetadata.storage_url.startswith(
                    "memory:"
                ),  # TODO: drawback of just having url for StorageResource instead of proper metadata object
                StoredDataResourceMetadata.storage_url
                == self.ctx.local_memory_storage.url,
            ),
        )
        existing_sdrs.filter(
            StoredDataResourceMetadata.storage_url.in_(self.ctx.all_storages),
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
        existing_sdrs = list(existing_sdrs)
        for sdr in existing_sdrs:
            source_storage_format = StorageFormat(
                sdr.storage.storage_type, sdr.data_format
            )
            if source_storage_format == target_storage_format:
                return sdr
            conversion_path = get_conversion_path_for_sdr(
                sdr, target_storage_format, self.ctx.all_storages
            )
            if conversion_path is not None:
                eligible_conversion_paths.append(
                    (conversion_path.total_cost, conversion_path, sdr)
                )
        if not eligible_conversion_paths:
            raise NotImplementedError(
                f"No converter to {target_format} for existing StoredDataResources {existing_sdrs}. {self}"
            )
        cost, conversion_path, in_sdr = min(
            eligible_conversion_paths, key=lambda x: x[0]
        )
        return convert_sdr(self.ctx, in_sdr, conversion_path)
