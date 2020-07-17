from __future__ import annotations

from collections import abc
from typing import Any, Dict, Generic, List, Optional, Type

from basis import ObjectType
from basis.core.data_formats import get_records_list_sample
from basis.core.data_formats.base import (
    DataFormatBase,
    MemoryDataFormatBase,
    ReusableGenerator,
)
from basis.core.data_formats.records_list import RecordsList


class RecordsListGenerator(ReusableGenerator[RecordsList]):
    pass


class RecordsListGeneratorFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls) -> Type:
        return RecordsListGenerator
    
    @classmethod
    def maybe_instance(cls, obj: Any) -> bool:
        return isinstance(obj, abc.Generator)

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        return isinstance(obj, cls.type())

    @classmethod
    def infer_otype_from_records(cls, records: RecordsListGenerator) -> ObjectType:
        from basis.core.typing.inference import infer_otype_from_records_list

        dl = get_records_list_sample(records)
        if dl is None:
            raise ValueError("Empty records object") # TODO
        inferred_otype = infer_otype_from_records_list(dl)
        return inferred_otype
