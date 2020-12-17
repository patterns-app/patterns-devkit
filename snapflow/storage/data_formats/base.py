from __future__ import annotations

import typing
from collections import abc
from copy import deepcopy
from itertools import tee
from typing import TYPE_CHECKING, Any, Dict, Generic, Iterator, List, Optional, Type

from snapflow.utils.data import (
    SampleableCursor,
    SampleableGenerator,
    SampleableIO,
    SampleableIterator,
)
from snapflow.utils.registry import ClassBasedEnum
from snapflow.utils.typing import T

if TYPE_CHECKING:
    from snapflow.storage.data_records import MemoryDataRecords
    from snapflow.schema import SchemaTranslation, Schema


class DataFormatBase(ClassBasedEnum, Generic[T]):
    def __init__(self):
        raise NotImplementedError("Do not instantiate DataFormat classes")

    @classmethod
    def is_python_format(cls) -> bool:
        return False

    @classmethod
    def is_storable(cls) -> bool:
        # Does the format store data in a stable, serializable format?
        # Examples that arent' storable: generators, file objects, and
        # db cursors -- they depend on open in-memory resources that may go away
        return True

    @classmethod
    def empty(cls) -> T:
        raise NotImplementedError

    @classmethod
    def type(cls) -> Type[T]:
        raise NotImplementedError

    @classmethod
    def type_hint(cls) -> str:
        return cls.__name__[:-6]  # Just trim "Format" from the class name, as default

    @classmethod
    def apply_schema_translation(cls, translation: SchemaTranslation, obj: T) -> T:
        raise NotImplementedError

    # TODO: these make sense for Memory only: Here for convenience?
    @classmethod
    def maybe_instance(cls, obj: Any) -> bool:
        raise NotImplementedError

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        raise NotImplementedError

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        raise NotImplementedError

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        raise NotImplementedError

    @classmethod
    def infer_schema_from_records(cls, records: T) -> Schema:
        raise NotImplementedError

    @classmethod
    def conform_records_to_schema(cls, records: T, schema: Schema) -> T:
        raise NotImplementedError


class FileDataFormatBase(DataFormatBase[T]):
    pass


class MemoryDataFormatBase(DataFormatBase[T]):
    @classmethod
    def is_python_format(cls) -> bool:
        return True

    @classmethod
    def empty(cls) -> T:
        return cls.type()()

    @classmethod
    def type(cls) -> Type[T]:
        raise NotImplementedError

    @classmethod
    def maybe_instance(cls, obj: Any) -> bool:
        return isinstance(obj, cls.type())

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        return False

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        return None

    @classmethod
    def get_records_sample(cls, obj: Any, n: int = 200) -> Optional[List[Dict]]:
        return None

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        if hasattr(obj, "copy") and callable(obj.copy):
            return obj.copy()
        else:
            return deepcopy(obj)

    @classmethod
    def infer_schema_from_records(cls, records: T) -> Schema:
        from snapflow.core.typing.inference import infer_schema_from_records

        dl = cls.get_records_sample(records)
        if dl is None:
            raise ValueError("Empty records object")
        inferred_schema = infer_schema_from_records(dl)
        return inferred_schema

    @classmethod
    def conform_records_to_schema(cls, records: T, schema: Schema) -> T:
        return records


DataFormat = Type[DataFormatBase]
MemoryDataFormat = Type[MemoryDataFormatBase]


class IteratorFormatBase(MemoryDataFormatBase):
    iterator_cls: Type
    object_format: DataFormat

    @classmethod
    def type(cls) -> Type:
        return cls.iterator_cls

    @classmethod
    def is_storable(cls) -> bool:
        return False

    @classmethod
    def get_records_sample(cls, obj: Any, n: int = 200) -> Optional[List[Dict]]:
        if isinstance(obj, SampleableIterator):
            o = obj.get_first()
            return cls.object_format.get_records_sample(o, n)
        return None

    @classmethod
    def maybe_instance(cls, obj: Any) -> bool:
        if not isinstance(obj, abc.Iterator):
            return False
        if isinstance(obj, SampleableIterator):
            one = obj.get_first()
            return cls.object_format.maybe_instance(one)
        return True

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        if isinstance(obj, SampleableIterator):
            one = obj.get_first()
            return cls.object_format.definitely_instance(one)
        return False

    @classmethod
    def apply_schema_translation(
        cls, translation: SchemaTranslation, obj: SampleableIterator
    ) -> Iterator:
        for records in obj:
            yield cls.object_format.apply_schema_translation(translation, records)

    @classmethod
    def conform_records_to_schema(
        cls, obj: SampleableIterator, schema: Schema
    ) -> Iterator:
        for records in obj:
            yield cls.object_format.conform_records_to_schema(records, schema)


def make_corresponding_iterator_format(fmt: DataFormat) -> DataFormat:
    name = fmt.__name__[:-6] + "IteratorFormat"
    iterator_cls = SampleableIterator  # TODO: specific classes?
    return type(
        name,
        (IteratorFormatBase,),
        {"iterator_cls": iterator_cls, "object_format": fmt},
    )
