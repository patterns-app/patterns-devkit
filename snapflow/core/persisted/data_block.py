from __future__ import annotations

from pydantic_sqlalchemy import sqlalchemy_to_pydantic
from dataclasses import dataclass
from snapflow.core.component import ComponentLibrary
from typing import (
    List,
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    Optional,
    Tuple,
    Type,
    Union,
)

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
from snapflow.core.persisted.base import (
    BaseModel,
    DataFormatType,
    timestamp_increment_key,
)
from snapflow.utils.registry import ClassBasedEnumSqlalchemyType
from snapflow.utils.typing import T
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, select
from sqlalchemy.orm import RelationshipProperty, Session, relationship
from sqlalchemy.sql.schema import UniqueConstraint


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

    # def as_managed_data_block(
    #     self, env: Environment, schema_translation: Optional[SchemaTranslation] = None,
    # ):
    #     mgr = DataBlockManager(env, self, schema_translation=schema_translation)
    #     return ManagedDataBlock(
    #         data_block_id=self.id,
    #         inferred_schema_key=self.inferred_schema_key,
    #         nominal_schema_key=self.nominal_schema_key,
    #         realized_schema_key=self.realized_schema_key,
    #         manager=mgr,
    #     )

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


def make_sdb_name(id: str, node_key: str = None) -> str:
    node_key = node_key or ""
    return as_identifier(f"_{node_key[:30]}_{id}")


def make_sdb_name_sa(context) -> str:
    id = context.get_current_parameters()["id"]
    node_key = context.get_current_parameters()["data_block"].created_by_node_key
    return make_sdb_name(id, node_key)


class StoredDataBlockMetadata(BaseModel):
    # id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String(128), primary_key=True, default=get_stored_datablock_id)
    # id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False, default=make_sdb_name_sa)
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
    data_is_written: bool = False

    def __repr__(self):
        return self._repr(
            id=self.id,
            data_block=self.data_block,
            data_format=self.data_format,
            storage_url=self.storage_url,
        )

    def get_handler(self) -> FormatHandler:
        return get_handler_for_name(self.name, self.storage)()

    def inferred_schema(self, lib: ComponentLibrary) -> Optional[Schema]:
        if self.data_block.inferred_schema_key is None:
            return None
        return lib.get_schema(self.data_block.inferred_schema_key)

    def nominal_schema(self, lib: ComponentLibrary) -> Optional[Schema]:
        if self.data_block.nominal_schema_key is None:
            return None
        return lib.get_schema(self.data_block.nominal_schema_key)

    def realized_schema(self, lib: ComponentLibrary) -> Schema:
        if self.data_block.realized_schema_key is None:
            return None
        return lib.get_schema(self.data_block.realized_schema_key)

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
        self.data_block.record_count = self.storage.get_api().record_count(
            self.get_name_for_storage()
        )
        return self.data_block.record_count

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

    __table_args__ = (UniqueConstraint("dataspace_key", "name"),)

    def update_alias(self, env: Environment, new_alias: str):
        self.stored_data_block.storage.get_api().create_alias(
            self.stored_data_block.get_name_for_storage(), new_alias
        )
        self.stored_data_block.storage.get_api().remove_alias(self.name)
        self.name = new_alias
        env.md_api.add(self)


# Type aliases

