from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from snapflow.core.data_formats.base import (
    MemoryDataFormatBase,
    make_corresponding_iterator_format,
)

if TYPE_CHECKING:
    from snapflow import Schema
    from snapflow.core.typing.schema import SchemaTranslation


RecordsList = List[Dict[str, Any]]


def map_recordslist(mapping: Dict[str, str], records: RecordsList) -> RecordsList:
    mapped = []
    for r in records:
        mapped.append({mapping.get(k, k): v for k, v in r.items()})
    return mapped


class RecordsListFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls):
        return list

    @classmethod
    def type_hint(cls) -> str:
        return "RecordsList"

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        if obj is None:
            return None
        return len(obj)

    @classmethod
    def get_records_sample(cls, obj: Any, n: int = 200) -> Optional[List[Dict]]:
        return obj[:n]

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
    def definitely_instance(cls, obj: Any) -> bool:
        return isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict)

    @classmethod
    def apply_schema_translation(
        cls, translation: SchemaTranslation, records: RecordsList
    ) -> RecordsList:
        m = translation.as_dict()
        return map_recordslist(m, records)


RecordsListIteratorFormat = make_corresponding_iterator_format(RecordsListFormat)
RecordsListIterator = Iterator[RecordsList]
