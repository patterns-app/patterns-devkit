from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import dcp
import sqlalchemy
from basis.core.component import ComponentLibrary, global_library
from basis.core.declarative.base import FrozenPydanticBase, PydanticBase
from basis.core.persistence.pydantic import (
    BlockMetadataCfg,
    BlockWithStoredBlocksCfg,
    StoredBlockMetadataCfg,
)
from basis.core.storage import ensure_block_on_storage_cfg
from commonmodel.base import AnySchema, Schema, SchemaLike, SchemaTranslation
from dcp.data_format.base import DataFormat, get_format_for_nickname
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.base import (
    FileSystemStorageClass,
    MemoryStorageClass,
    Storage,
    ensure_storage,
)
from dcp.storage.memory.engines.python import new_local_python_storage
from dcp.utils.common import rand_str, utcnow
from loguru import logger
from pandas.core.frame import DataFrame

if TYPE_CHECKING:
    from basis.core.execution.context import Context


class BlockManager:
    def __init__(
        self,
        block: BlockWithStoredBlocksCfg,
        ctx: Context = None,
        schema_translation: Optional[SchemaTranslation] = None,
        storages: List[str] = None,
    ):
        self.ctx = ctx
        self.library = self.ctx.library if self.ctx else global_library
        self.block = block
        self.stored_blocks = self.block.stored_blocks
        self.storages = storages or []
        if not self.storages and self.ctx is not None:
            storages = self.ctx.execution_config.storages
        if not self.storages:
            self.storages = [new_local_python_storage()]
        self.schema_translation = schema_translation

    def __getattr__(self, name: str) -> Any:
        return getattr(self.block, name)

    @property
    def realized_schema(self) -> Schema:
        return self.library.get_schema(self.block.realized_schema_key)

    @property
    def nominal_schema(self) -> Optional[Schema]:
        return self.library.get_schema(self.block.nominal_schema_key)

    @property
    def inferred_schema(self) -> Optional[Schema]:
        return self.library.get_schema(self.block.inferred_schema_key)

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
    ) -> StoredBlockMetadataCfg:
        from basis.core.storage import ensure_block_on_storage_cfg

        storages = [ensure_storage(s) for s in self.storages]

        if fmt.natural_storage_class == MemoryStorageClass:
            # Ensure we are putting memory format in memory
            # if not target_storage.storage_engine.storage_class == PythonStorageClass:
            for s in storages:
                if s.storage_engine.storage_class == MemoryStorageClass:
                    target_storage = s
        assert target_storage is not None
        sdbs = ensure_block_on_storage_cfg(
            block=self.block,
            storage=target_storage,
            stored_blocks=self.stored_blocks,
            fmt=fmt,
            eligible_storages=storages,
        )
        if self.ctx is not None:
            for sdb in sdbs:
                self.ctx.add_stored_block(sdb)
        return sdbs[0]

    def as_python_object(self, sdb: StoredBlockMetadataCfg) -> Any:
        if self.schema_translation:
            sdb.get_handler().apply_schema_translation(
                sdb.name, sdb.storage, self.schema_translation
            )
        if sdb.data_format.natural_storage_class == MemoryStorageClass:
            obj = sdb.storage.get_api().get(sdb.name)
        else:
            if sdb.data_format == DatabaseTableFormat:
                # TODO:
                # obj = DatabaseTableRef(sdb.get_name(), storage_url=sdb.storage.url)
                # raise NotImplementedError
                return sdb.name
            else:
                # TODO: what is general solution to this? if we do DataFormat.as_python_object(sdb) then
                #       we have formats depending on StoredBlocks again, and no seperation of concerns
                #       BUT what other option is there? Need knowledge of storage url and name to make useful "pointer" object
                # raise NotImplementedError(
                #     f"Don't know how to bring '{sdb.data_format}' into python'"
                # )
                return sdb.name
        return obj

    def has_format(self, fmt: DataFormat) -> bool:
        return fmt in [s.data_format for s in self.stored_blocks]

    def as_sql_from_stmt(self, storage: Storage) -> str:
        from basis.core.sql.sql_function import apply_schema_translation_as_sql

        # TODO: this feels pretty forced -- how do we do schema transations in a general way for non-memory storages / runtimes?
        quote_identifier = storage.get_api().get_quoted_identifier
        sql = self.as_table(storage)
        if self.schema_translation:
            sql = apply_schema_translation_as_sql(
                self.library,
                sql,
                self.schema_translation,
                quote_identifier=quote_identifier,
            )
        return sql


Block = BlockManager


def as_managed(db: BlockWithStoredBlocksCfg, **kwargs) -> Block:
    return BlockManager(block=db, **kwargs)
