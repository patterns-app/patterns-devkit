from __future__ import annotations

from typing import Any, List

from basis.core.persistence.block import (
    Alias,
    BlockMetadata,
    StoredBlockMetadata,
)

from basis.core.persistence.state import ExecutionLog
from dcp.data_format.base import DataFormat, get_format_for_nickname
from dcp.storage.base import Storage
from pydantic import validator
from pydantic_sqlalchemy.main import sqlalchemy_to_pydantic

_BlockMetadataCfg = sqlalchemy_to_pydantic(BlockMetadata)
_StoredBlockMetadataCfg = sqlalchemy_to_pydantic(StoredBlockMetadata)
AliasCfg = sqlalchemy_to_pydantic(Alias)


class BlockMetadataCfg(_BlockMetadataCfg):
    data_is_written: bool = False


class StoredBlockMetadataCfg(_StoredBlockMetadataCfg):
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


class BlockWithStoredBlocksCfg(BlockMetadataCfg):
    stored_blocks: List[StoredBlockMetadataCfg] = []


ExecutionLogCfg = sqlalchemy_to_pydantic(ExecutionLog)
# BlockLogCfg = sqlalchemy_to_pydantic(BlockLog)
# NodeStateCfg = sqlalchemy_to_pydantic(NodeState)
