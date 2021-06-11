from __future__ import annotations

from typing import Any, List

from dcp.data_format.base import DataFormat, get_format_for_nickname
from dcp.storage.base import Storage
from pydantic import validator
from pydantic_sqlalchemy.main import sqlalchemy_to_pydantic
from snapflow.core.persistence.data_block import (
    Alias,
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from snapflow.core.persistence.state import DataBlockLog, DataFunctionLog, NodeState

_DataBlockMetadataCfg = sqlalchemy_to_pydantic(DataBlockMetadata)
_StoredDataBlockMetadataCfg = sqlalchemy_to_pydantic(StoredDataBlockMetadata)
AliasCfg = sqlalchemy_to_pydantic(Alias)


class DataBlockMetadataCfg(_DataBlockMetadataCfg):
    data_is_written: bool = False


class StoredDataBlockMetadataCfg(_StoredDataBlockMetadataCfg):
    data_format: DataFormat
    data_is_written: bool = False

    @validator("data_format", pre=True)
    def ensure_dataformat(cls, v: Any) -> DataFormat:
        if isinstance(v, str):
            return get_format_for_nickname(v)
        return v

    @property
    def storage(self) -> Storage:
        return Storage(self.storage_url)


class DataBlockWithStoredBlocksCfg(DataBlockMetadataCfg):
    stored_data_blocks: List[StoredDataBlockMetadataCfg] = []


DataFunctionLogCfg = sqlalchemy_to_pydantic(DataFunctionLog)
DataBlockLogCfg = sqlalchemy_to_pydantic(DataBlockLog)
NodeStateCfg = sqlalchemy_to_pydantic(NodeState)
