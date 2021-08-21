from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from basis.core.component import ComponentLibrary
from basis.core.environment import Environment
from basis.core.persistence.base import (
    BaseModel,
    DataFormatType,
    timestamp_increment_key,
)
from basis.utils.registry import ClassBasedEnumSqlalchemyType
from basis.utils.typing import T
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
from pydantic_sqlalchemy import sqlalchemy_to_pydantic
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, select
from sqlalchemy.orm import RelationshipProperty, Session, relationship
from sqlalchemy.sql.schema import UniqueConstraint

if TYPE_CHECKING:
    from basis.core.persistence.pydantic import (
        BlockMetadataCfg,
        StoredBlockMetadataCfg,
        BlockWithStoredBlocksCfg,
    )
    from basis.core.block import Block


def get_block_id(emitting_node_key: str) -> str:
    return timestamp_increment_key(prefix=emitting_node_key)


def ensure_lib(lib: Union[Environment, ComponentLibrary]) -> ComponentLibrary:
    if isinstance(lib, Environment):
        return lib.library
    return lib


class BlockMetadata(BaseModel):  # , Generic[DT]):
    # NOTE on block ids: we generate them dynamically so we don't have to hit a central db for a sequence
    # BUT we MUST ensure they are monotonically ordered -- the logic of selecting the correct (most recent)
    # block relies on strict monotonic IDs in some scenarios
    id = Column(String(128), primary_key=True, default=get_block_id)
    # id = Column(Integer, primary_key=True, autoincrement=True)
    # start_block_id = Column(String(128), index=True)
    # end_block_id = Column(String(128), index=True)
    # block_count = Column(Integer, default=1)
    inferred_schema_key: SchemaKey = Column(String(128), nullable=True)  # type: ignore
    nominal_schema_key: SchemaKey = Column(String(128), nullable=True)  # type: ignore
    realized_schema_key: SchemaKey = Column(String(128), nullable=False)  # type: ignore
    record_count = Column(Integer, nullable=True)
    created_by_node_key = Column(String(128), nullable=True)
    # Other metadata? created_by_job? last_processed_at?
    deleted = Column(Boolean, default=False)
    stored_blocks = relationship("StoredBlockMetadata", backref="block", lazy="dynamic")
    data_is_written: bool = False
    aliases: RelationshipProperty = relationship(
        "Alias", backref="block", lazy="dynamic"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            inferred_schema_key=self.inferred_schema_key,
            nominal_schema_key=self.nominal_schema_key,
            realized_schema_key=self.realized_schema_key,
            record_count=self.record_count,
        )

    def to_pydantic(self) -> BlockMetadataCfg:
        from basis.core.persistence.pydantic import BlockMetadataCfg

        return BlockMetadataCfg.from_orm(self)

    def to_pydantic_with_stored(self) -> BlockWithStoredBlocksCfg:
        from basis.core.persistence.pydantic import BlockWithStoredBlocksCfg

        db = self.to_pydantic()
        return BlockWithStoredBlocksCfg(
            **db.dict(),
            stored_blocks=[s.to_pydantic() for s in self.stored_blocks.all()],
        )

        # return BlockWithStoredBlocksCfg.from_orm(self)

    def as_managed_block(
        self, env: Environment, schema_translation: Optional[SchemaTranslation] = None,
    ) -> Block:
        from basis.core.block import BlockManager

        return BlockManager(
            self.to_pydantic_with_stored(),
            schema_translation=schema_translation,
            storages=[s.url for s in env.get_storages()],
        )

    def compute_record_count(self) -> bool:
        for sdb in self.stored_blocks.all():
            cnt = sdb.record_count()
            if cnt is not None:
                self.record_count = cnt
                return True
        return False

    # def created_by(self: Session) -> Optional[str]:
    #     from basis.core.node import BlockLog
    #     from basis.core.node import FunctionLog
    #     from basis.core.node import Direction

    #     result = (
    #         env.md_api.execute(select(FunctionLog.node_key)
    #         .join(BlockLog)
    #         .filter(
    #             BlockLog.direction == Direction.OUTPUT,
    #             BlockLog.block_id == self.id,
    #         )
    #         .scalar_one_or_none()
    #     )
    #     if result:
    #         return result[0]
    #     return None


def make_sdb_name(id: str, node_key: str = None) -> str:
    node_key = node_key or ""
    return as_identifier(f"_{node_key[:30]}_{id}")


# def make_sdb_name_sa(context) -> str:
#     id = context.get_current_parameters()["id"]
#     node_key = context.get_current_parameters()["block"].created_by_node_key
#     return make_sdb_name(id, node_key)


class StoredBlockMetadata(BaseModel):
    # id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String(128), primary_key=True, default=get_block_id)
    # id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)
    block_id = Column(String(128), ForeignKey(BlockMetadata.id), nullable=False)
    storage_url = Column(String(128), nullable=False)
    data_format: DataFormat = Column(DataFormatType, nullable=False)  # type: ignore
    aliases: RelationshipProperty = relationship(
        "Alias", backref="stored_block", lazy="dynamic"
    )
    # is_ephemeral = Column(Boolean, default=False) # TODO
    # Hints
    block: "BlockMetadata"
    data_is_written: bool = False

    def __repr__(self):
        return self._repr(
            id=self.id,
            block=self.block,
            data_format=self.data_format,
            storage_url=self.storage_url,
        )

    def to_pydantic(self) -> StoredBlockMetadataCfg:
        from basis.core.persistence.pydantic import StoredBlockMetadataCfg

        return StoredBlockMetadataCfg.from_orm(self)

    @classmethod
    def from_pydantic(cls, cfg: StoredBlockMetadataCfg) -> StoredBlockMetadata:
        return StoredBlockMetadata(**cfg.dict())

    def get_handler(self) -> FormatHandler:
        return get_handler_for_name(self.name, self.storage)()

    @property
    def storage(self) -> Storage:
        return Storage.from_url(self.storage_url)

    def get_storage_format(self) -> StorageFormat:
        return StorageFormat(self.storage.storage_engine, self.data_format)

    def exists(self) -> bool:
        return self.storage.get_api().exists(self.name)

    def record_count(self) -> Optional[int]:
        if self.block.record_count is not None:
            return self.block.record_count
        self.block.record_count = self.storage.get_api().record_count(self.name)
        return self.block.record_count

    def get_alias(self, env: Environment) -> Optional[Alias]:
        return env.md_api.execute(
            select(Alias).filter(Alias.stored_block_id == self.id)
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
            select(Alias).filter(
                Alias.name == alias, Alias.storage_url == self.storage_url
            )
        ).scalar_one_or_none()
        if a is None:
            # (not really a race condition here since alias is unique to node and node cannot
            #  run in parallel, for now at least)
            a = Alias(
                name=alias,
                block_id=self.block_id,
                stored_block_id=self.id,
                storage_url=self.storage_url,
            )
            env.md_api.add(a)
        else:
            a.block_id = self.block_id
            a.stored_block_id = self.id
            a.storage_url = self.storage_url
        self.storage.get_api().create_alias(self.name, alias)
        return a


# event.listen(BlockMetadata, "before_update", immutability_update_listener)
# event.listen(StoredBlockMetadata, "before_update", add_persisting_sdb_listener)


class Alias(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128))
    storage_url = Column(String(128), nullable=True)
    block_id = Column(String(128), ForeignKey(BlockMetadata.id), nullable=False)
    stored_block_id = Column(
        String(128), ForeignKey(StoredBlockMetadata.id), nullable=False
    )
    # Hints
    block: "BlockMetadata"
    stored_block: "StoredBlockMetadata"

    __table_args__ = (UniqueConstraint("env_id", "name", "storage_url"),)

    def update_alias(self, env: Environment, new_alias: str):
        self.stored_block.storage.get_api().create_alias(
            self.stored_block.name, new_alias
        )
        self.stored_block.storage.get_api().remove_alias(self.name)
        self.name = new_alias
        env.md_api.add(self)


# Type aliases
