from __future__ import annotations

from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Type,
)

import pandas as pd
from sqlalchemy.engine import ResultProxy

from basis.utils.common import StringEnum
from basis.utils.registry import T

if TYPE_CHECKING:
    from basis.core.storage_resource import StorageClass


class DataFormat(StringEnum):
    # Storage
    JSON_LIST_FILE = "json_list_file"
    DELIMITED_FILE = "delimited_file"
    DATABASE_TABLE = "database_table"
    # Memory
    JSON_LIST = "json_list"
    DICT_LIST = "dict_list"
    DELIMITED_FILE_POINTER = "delimited_file_pointer"
    DATABASE_CURSOR = "database_cursor"
    DATABASE_TABLE_REF = "database_table_ref"
    DATAFRAME = "dataframe"
    DATAFRAME_ITERATOR = "dataframe_iterator"
    DICT_LIST_ITERATOR = "dict_list_iterator"

    def get_manager(self) -> Type[DataFormatManager]:
        return data_format_managers[self]

    def memory_formats(self) -> Sequence[DataFormat]:
        return (
            DataFormat.JSON_LIST,
            DataFormat.DICT_LIST,
            DataFormat.DELIMITED_FILE_POINTER,
            DataFormat.DATABASE_CURSOR,
            DataFormat.DATABASE_TABLE_REF,
            DataFormat.DATAFRAME,
            DataFormat.DATAFRAME_ITERATOR,
            DataFormat.DICT_LIST_ITERATOR,
        )

    def is_memory_format(self) -> bool:
        return self in self.memory_formats()

    def get_natural_storage_class(self) -> StorageClass:
        from basis.core.storage_resource import NATURAL_STORAGE_CLASS

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


class DictListFormat(DataFormatManager):
    data_format = DataFormat.DICT_LIST

    @classmethod
    def type(cls):
        return list

    @classmethod
    def type_hint(cls) -> str:
        return "DictList"

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


class DictListIteratorFormat(DataFormatManager):
    data_format = DataFormat.DICT_LIST_ITERATOR

    @classmethod
    def type_hint(cls) -> str:
        return "DictListIterator"

    @classmethod
    def isinstance(cls, obj: Any) -> bool:
        # TODO: hmm how to check iterator type?
        #   Maybe make dedicated class?
        return isinstance(obj, Iterator)

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        # Not applicable to iterator
        return obj


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
    def __init__(self, table_name: str, storage_resource_url: str):
        self.table_name = table_name
        self.storage_resource_url = storage_resource_url

    def __repr__(self):
        return f"{self.storage_resource_url}/{self.table_name}"


class DatabaseTableRefFormat(DataFormatManager):
    data_format = DataFormat.DATABASE_TABLE_REF

    @classmethod
    def type(cls):
        return DatabaseTable

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        # Not applicable to database table ref
        return obj


JSONList = List[str]
DictList = List[Dict[str, Any]]
DelimitedFilePointer = Any  # TODO ??
DatabaseCursor = Any  # TODO
DictListIterator = Iterator[DictList]


all_managers = [
    DataFrameFormat,
    DictListFormat,
    DatabaseCursorFormat,
    DatabaseTableRefFormat,
    DictListIteratorFormat,
]
data_format_managers = {m.data_format: m for m in all_managers}


def get_data_format_of_object(obj: Any) -> Optional[DataFormat]:
    for m in all_managers:
        if m.isinstance(obj):
            return m.data_format
    return None
