from __future__ import annotations

from typing import Any, Dict, Generic, List, Optional, Type

from basis.core.data_formats.base import MemoryDataFormatBase


class DatabaseTableRef:
    def __init__(self, table_name: str, storage_url: str):
        self.table_name = table_name
        self.storage_url = storage_url

    def __repr__(self):
        return f"{self.storage_url}/{self.table_name}"


class DatabaseTableRefFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls):
        return DatabaseTableRef

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        # Not applicable to database table ref
        return obj
