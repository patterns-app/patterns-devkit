from __future__ import annotations

import collections.abc
import typing
from copy import deepcopy
from io import BytesIO
from itertools import tee
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Type,
    Union,
)

import pandas as pd
from sqlalchemy.engine import ResultProxy

from basis.utils.common import StringEnum
from basis.utils.typing import T

if TYPE_CHECKING:
    from basis.core.storage.storage import StorageClass


class DataFormat(StringEnum):
    # Storage
    JSON_LIST_FILE = "JSON_LIST_FILE"
    DELIMITED_FILE = "DELIMITED_FILE"
    DATABASE_TABLE = "DATABASE_TABLE"
    # Memory
    JSON_LIST = "JSON_LIST"
    RECORDS_LIST = "RECORDS_LIST"
    DELIMITED_FILE_POINTER = "DELIMITED_FILE_POINTER"
    DATABASE_CURSOR = "DATABASE_CURSOR"
    DATABASE_TABLE_REF = "DATABASE_TABLE_REF"
    DATAFRAME = "DATAFRAME"
    DATAFRAME_GENERATOR = "DATAFRAME_GENERATOR"
    RECORDS_LIST_GENERATOR = "RECORDS_LIST_GENERATOR"

    def get_manager(self) -> Type[DataFormatManager]:
        return data_format_managers[self]

    def memory_formats(self) -> Sequence[DataFormat]:
        return (
            DataFormat.JSON_LIST,
            DataFormat.RECORDS_LIST,
            DataFormat.DELIMITED_FILE_POINTER,
            DataFormat.DATABASE_CURSOR,
            DataFormat.DATABASE_TABLE_REF,
            DataFormat.DATAFRAME,
            DataFormat.DATAFRAME_GENERATOR,
            DataFormat.RECORDS_LIST_GENERATOR,
        )

    def is_memory_format(self) -> bool:
        return self in self.memory_formats()

    def get_natural_storage_class(self) -> StorageClass:
        from basis.core.storage.storage import NATURAL_STORAGE_CLASS

        return NATURAL_STORAGE_CLASS[self]


class DataFormatManager(Generic[T]):
    data_format: DataFormat

    @classmethod
    def empty(cls) -> T:
        return cls.type()()

    @classmethod
    def type(cls) -> Type[T]:
        raise NotImplementedError

    @classmethod
    def type_hint(cls) -> str:
        return cls.type().__name__

    @classmethod
    def isinstance(cls, obj: Any) -> bool:
        return isinstance(obj, cls.type())

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        return None

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        if hasattr(obj, "copy") and callable(obj.copy):
            return obj.copy()
        else:
            return deepcopy(obj)


class DataFrameFormat(DataFormatManager):
    data_format = DataFormat.DATAFRAME

    @classmethod
    def type(cls):
        return pd.DataFrame

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        if obj is None:
            return None
        return len(obj)


class RecordsListFormat(DataFormatManager):
    data_format = DataFormat.RECORDS_LIST

    @classmethod
    def type(cls):
        return list

    @classmethod
    def type_hint(cls) -> str:
        return "RecordsList"

    @classmethod
    def isinstance(cls, obj: Any) -> bool:
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


class RecordsListGeneratorFormat(DataFormatManager):
    data_format = DataFormat.RECORDS_LIST_GENERATOR

    @classmethod
    def type(cls) -> Type:
        return RecordsListGenerator


class DataFrameGeneratorFormat(DataFormatManager):
    data_format = DataFormat.DATAFRAME_GENERATOR

    @classmethod
    def type(cls) -> Type:
        return DataFrameGenerator


class DatabaseCursorFormat(DataFormatManager):
    data_format = DataFormat.DATABASE_CURSOR

    @classmethod
    def empty(cls):
        raise NotImplementedError

    @classmethod
    def type(cls) -> Type:
        return ResultProxy

    @classmethod
    def type_hint(cls):
        return "DatabaseCursor"

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        # Not applicable to cursor
        return obj


class DatabaseTable:
    def __init__(self, table_name: str, storage_url: str):
        self.table_name = table_name
        self.storage_url = storage_url

    def __repr__(self):
        return f"{self.storage_url}/{self.table_name}"


class DatabaseTableRefFormat(DataFormatManager):
    data_format = DataFormat.DATABASE_TABLE_REF

    @classmethod
    def type(cls):
        return DatabaseTable

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        # Not applicable to database table ref
        return obj


class FilePointer:
    def __init__(self, file_like: BytesIO):
        self.file_like = file_like


class DelimitedFilePointer(FilePointer):
    pass


# Would we ever really want to use this...?
# class JsonListFileFormat(DataFormatManager):
#     data_format = DataFormat.JSON_LIST_FILE_POINTER
#
#     @classmethod
#     def type(cls):
#         return JsonFilePointer


class DelimitedFilePointerFormat(DataFormatManager):
    data_format = DataFormat.DELIMITED_FILE_POINTER

    @classmethod
    def type(cls):
        return DelimitedFilePointer


JSONList = List[str]
RecordsList = List[Dict[str, Any]]
DatabaseCursor = Any  # TODO


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


class DataFrameGenerator(ReusableGenerator[pd.DataFrame]):
    pass


class RecordsListGenerator(ReusableGenerator[RecordsList]):
    pass


all_managers = [
    DataFrameFormat,
    RecordsListFormat,
    DatabaseCursorFormat,
    DatabaseTableRefFormat,
    RecordsListGeneratorFormat,
    DataFrameGeneratorFormat,
    DelimitedFilePointerFormat,
]
data_format_managers = {m.data_format: m for m in all_managers}


def get_data_format_of_object(obj: Any) -> Optional[DataFormat]:
    for m in all_managers:
        if m.isinstance(obj):
            return m.data_format
    return None


def get_records_list_sample(
    obj: Union[pd.DataFrame, RecordsList, RecordsListGenerator, DataFrameGenerator]
) -> Optional[RecordsList]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")
    if isinstance(obj, DataFrameGenerator):
        return get_records_list_sample(obj.get_one())
    if isinstance(obj, RecordsListGenerator):
        return obj.get_one()
    if isinstance(obj, collections.abc.Generator):
        raise TypeError("Generators must be `tee`d before being passed in")
    raise TypeError(obj)


# def conform_generator_format(g: Generator) -> ReusableGenerator:
#     if runnable.datafunction_interface.output.data_format_class == "DataFrameGenerator":
#         output = DataFrameGenerator(output)
#     if runnable.datafunction_interface.output.data_format_class == "RecordsListGenerator":
#         output = RecordsListGenerator(output)
