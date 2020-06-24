from __future__ import annotations

from typing import Any, Dict, Generic, List, Optional, Type

from basis.core.data_formats.base import DataFormatBase, MemoryDataFormatBase

RecordsList = List[Dict[str, Any]]


class RecordsListFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls):
        return list

    @classmethod
    def type_hint(cls) -> str:
        return "RecordsList"

    @classmethod
    def maybe_instance(cls, obj: Any) -> bool:
        if not isinstance(obj, cls.type()):
            return False
        if len(obj) == 0:
            return True
        if not isinstance(obj[0], dict):
            return False
        return True

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        if obj is None:
            return None
        return len(obj)
