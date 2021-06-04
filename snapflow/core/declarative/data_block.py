from __future__ import annotations

from enum import Enum
from snapflow.core.storage import ensure_data_block_on_storage_cfg
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
    name: str
    data_format: DataFormat
    data_is_written: bool = False
    _name_placeholder = "__nameph__"

    @validator("name", pre=True, always=True)
    def name_placeholder(self, v: Any) -> Any:
        if v is None:
            return self._name_placeholder
        return v

    @root_validator
    def ensure_name(self, values: Dict) -> Dict:
        if values.get("name") == self._name_placeholder:
            values["name"] = make_sdb_name(
                values["id"], values["data_block"].created_by_node_key
            )
        return values


def make_sdb_name(id: str, node_key: str = None) -> str:
    node_key = node_key or ""
    return as_identifier(f"_{node_key[:30]}_{id}")

