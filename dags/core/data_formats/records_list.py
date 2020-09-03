from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from dags.core.data_formats.base import MemoryDataFormatBase

if TYPE_CHECKING:
    from dags import ObjectType


RecordsList = List[Dict[str, Any]]


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
    def infer_otype_from_records(cls, records: RecordsList) -> ObjectType:
        from dags.core.typing.inference import infer_otype_from_records_list
        from dags.core.data_formats import get_records_list_sample

        dl = get_records_list_sample(records)
        if dl is None:
            raise ValueError("Empty records object")  # TODO
        inferred_otype = infer_otype_from_records_list(dl)
        return inferred_otype
