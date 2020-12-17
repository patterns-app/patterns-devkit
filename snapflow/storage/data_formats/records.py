from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from snapflow.storage.data_formats.base import (
    MemoryDataFormatBase,
    make_corresponding_iterator_format,
)

if TYPE_CHECKING:
    from snapflow.schema import Schema, SchemaTranslation


Records = List[Dict[str, Any]]


def map_recordslist(mapping: Dict[str, str], records: Records) -> Records:
    mapped = []
    for r in records:
        mapped.append({mapping.get(k, k): v for k, v in r.items()})
    return mapped


class RecordsFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls):
        return list

    @classmethod
    def type_hint(cls) -> str:
        return "Records"

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
        cls, translation: SchemaTranslation, records: Records
    ) -> Records:
        m = translation.as_dict()
        return map_recordslist(m, records)

    @classmethod
    def conform_records_to_schema(cls, records: Records, schema: Schema) -> Records:
        from snapflow.core.typing.inference import conform_records_to_schema

        return conform_records_to_schema(records, schema)


RecordsIteratorFormat = make_corresponding_iterator_format(RecordsFormat)
RecordsIterator = Iterator[Records]
