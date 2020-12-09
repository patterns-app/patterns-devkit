from __future__ import annotations

from collections import abc
from typing import TYPE_CHECKING, Any, Generator, Iterator, Optional, Type

from snapflow.core.data_formats.base import MemoryDataFormatBase, ReusableGenerator
from snapflow.core.data_formats.records_list import (
    RecordsList,
    RecordsListFormat,
    map_recordslist,
)

if TYPE_CHECKING:
    from snapflow.core.typing.schema import SchemaTranslation
    from snapflow import Schema


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
        if isinstance(obj, cls.type()):
            return True
        if isinstance(obj, ReusableGenerator):
            one = obj.get_one()
            return RecordsListFormat.definitely_instance(one)
        return False

    @classmethod
    def infer_schema_from_records(cls, records: RecordsListGenerator) -> Schema:
        from snapflow.core.typing.inference import infer_schema_from_records_list
        from snapflow.core.data_formats import get_records_list_sample

        dl = get_records_list_sample(records)
        if dl is None:
            raise ValueError("Empty records object")
        inferred_schema = infer_schema_from_records_list(dl)
        return inferred_schema

    @classmethod
    def apply_schema_translation(
        cls, translation: SchemaTranslation, rlg: RecordsListGenerator
    ) -> Iterator[RecordsList]:
        m = translation.as_dict()
        for records in rlg.get_generator():
            yield map_recordslist(m, records)
