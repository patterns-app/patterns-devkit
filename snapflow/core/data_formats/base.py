from __future__ import annotations

import typing
from copy import deepcopy
from itertools import tee
from typing import TYPE_CHECKING, Any, Generic, Optional, Type

from snapflow.utils.typing import T

if TYPE_CHECKING:
    from snapflow.core.data_block import LocalMemoryDataRecords
    from snapflow.core.typing.schema import SchemaTranslation
    from snapflow import Schema


class DataFormatBase(Generic[T]):
    def __init__(self):
        raise NotImplementedError("Do not instantiate DataFormat classes")

    @classmethod
    def is_memory_format(cls) -> bool:
        return False

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
    def conform_records_to_schema(cls, records: T) -> T:
        raise NotImplementedError

    @classmethod
    def apply_schema_translation(cls, translation: SchemaTranslation, obj: T) -> T:
        raise NotImplementedError


class MemoryDataFormatBase(DataFormatBase[T]):
    @classmethod
    def is_memory_format(cls) -> bool:
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
    def copy_records(cls, obj: Any) -> Any:
        if hasattr(obj, "copy") and callable(obj.copy):
            return obj.copy()
        else:
            return deepcopy(obj)


DataFormat = Type[DataFormatBase]
MemoryDataFormat = Type[MemoryDataFormatBase]


class ReusableGenerator(Generic[T]):
    def __init__(self, generator: typing.Generator):
        self._generator = generator

    def get_generator(self) -> typing.Generator:
        copy1, copy2 = tee(self._generator, 2)
        self._generator = typing.cast(typing.Generator, copy1)
        return typing.cast(typing.Generator, copy2)

    def get_one(self) -> Optional[T]:
        return next(self.get_generator(), None)

    def copy(self) -> ReusableGenerator[T]:
        return self.__class__(self.get_generator())
