from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from basis.configuration.base import FrozenPydanticBase


class StoredBlockCfg(FrozenPydanticBase):
    id: str
    block_id: str
    created_at: datetime
    updated_at: datetime
    storage_url: str
    data_format: str
    realized_schema_key: str
    inferred_schema_key: Optional[str] = None
    nominal_schema_key: Optional[str] = None
    immutable: bool = True
    record_count: Optional[int] = None


class TableCfg(FrozenPydanticBase):
    block_id: str
    project_id: str
    node_id: str
    output_name: str
    realized_schema_key: str
    inferred_schema_key: Optional[str] = None
    nominal_schema_key: Optional[str] = None
    deleted: bool = False
    record_count: Optional[int] = None
    stored_blocks: List[StoredBlockCfg] = []


class RecordSliceCfg(FrozenPydanticBase):
    block_id: str
    record_start_index_inclusive: str
    record_end_index_inclusive: Optional[str]
    project_id: str
    node_id: str
    output_name: str
    record_count: Optional[int] = None
    stored_blocks: List[StoredBlockCfg] = []
