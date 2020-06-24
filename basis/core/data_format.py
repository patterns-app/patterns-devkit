# from __future__ import annotations
#
# import collections.abc
# import typing
# from collections import OrderedDict
# from copy import deepcopy
# from io import BytesIO
# from itertools import tee
# from typing import (
#     TYPE_CHECKING,
#     Any,
#     Dict,
#     Generic,
#     List,
#     Optional,
#     Sequence,
#     Type,
#     Union,
# )
#
# import pandas as pd
# from sqlalchemy.engine import ResultProxy
#
# from basis.utils.common import StringEnum
# from basis.utils.typing import T
#
# if TYPE_CHECKING:
#     from basis.core.storage.storage import StorageClass
#
#
# class DataFrameFormat(DataFormatBase):
#     @classmethod
#     def type(cls):
#         return pd.DataFrame
#
#     @classmethod
#     def get_record_count(cls, obj: Any) -> Optional[int]:
#         if obj is None:
#             return None
#         return len(obj)
#
#
# class RecordsListFormat(DataFormatBase):
#     @classmethod
#     def type(cls):
#         return list
#
#     @classmethod
#     def type_hint(cls) -> str:
#         return "RecordsList"
#
#     @classmethod
#     def maybe_instance(cls, obj: Any) -> bool:
#         if not isinstance(obj, cls.type()):
#             return False
#         if len(obj) == 0:
#             return True
#         if not isinstance(obj[0], dict):
#             return False
#         return True
#
#     @classmethod
#     def get_record_count(cls, obj: Any) -> Optional[int]:
#         if obj is None:
#             return None
#         return len(obj)
#
#
# class RecordsListGeneratorFormat(DataFormatBase):
#     @classmethod
#     def type(cls) -> Type:
#         return RecordsListGenerator
#
#
# class DataFrameGeneratorFormat(DataFormatBase):
#     @classmethod
#     def type(cls) -> Type:
#         return DataFrameGenerator
#
#
# class DatabaseCursorFormat(DataFormatBase):
#     @classmethod
#     def empty(cls):
#         raise NotImplementedError
#
#     @classmethod
#     def type(cls) -> Type:
#         return (
#             ResultProxy  # TODO: assumes we are always using sqlalchemy. Which we are?
#         )
#
#     @classmethod
#     def type_hint(cls):
#         return "DatabaseCursor"
#
#     @classmethod
#     def copy_records(cls, obj: Any) -> Any:
#         # Not applicable to cursor
#         return obj
#
#
# class DatabaseTableRef:
#     def __init__(self, table_name: str, storage_url: str):
#         self.table_name = table_name
#         self.storage_url = storage_url
#
#     def __repr__(self):
#         return f"{self.storage_url}/{self.table_name}"
#
#
# class DatabaseTableRefFormat(DataFormatBase):
#     @classmethod
#     def type(cls):
#         return DatabaseTableRef
#
#     @classmethod
#     def copy_records(cls, obj: Any) -> Any:
#         # Not applicable to database table ref
#         return obj
#
#
# class FilePointer:
#     def __init__(self, file_like: BytesIO):
#         self.file_like = file_like
#
#
# class DelimitedFilePointer(FilePointer):
#     pass
#
#
# # Would we ever really want to use this...?
# # class JsonListFileFormat(DataFormatManager):
# #     data_format = JsonListFileFormat_POINTER
# #
# #     @classmethod
# #     def type(cls):
# #         return JsonFilePointer
#
#
# class DelimitedFilePointerFormat(DataFormatBase):
#     @classmethod
#     def type(cls):
#         return DelimitedFilePointer
#
#
# JSONList = List[str]
# RecordsList = List[Dict[str, Any]]
# DatabaseCursor = Any  # TODO
#
#
# class ReusableGenerator(Generic[T]):
#     def __init__(self, generator: typing.Generator):
#         self._generator = generator
#
#     def get_generator(self) -> typing.Generator:
#         copy1, copy2 = tee(self._generator, 2)
#         self._generator = typing.cast(typing.Generator, copy1)
#         return typing.cast(typing.Generator, copy2)
#
#     def get_one(self) -> Optional[T]:
#         return next(self.get_generator(), None)
#
#     def copy(self) -> ReusableGenerator[T]:
#         return self.__class__(self.get_generator())
#
#
# class DataFrameGenerator(ReusableGenerator[pd.DataFrame]):
#     pass
#
#
# class RecordsListGenerator(ReusableGenerator[RecordsList]):
#     pass
#
#
# class DataFormatRegistry:
#     _registry: Dict[str, DataFormat] = {}
#
#     def register(self, fmt_cls: DataFormat):
#         self._registry[str(fmt_cls)] = fmt_cls
#
#     def get(self, key: str) -> DataFormat:
#         return self._registry.get(key)
#
#     def all(self) -> typing.Iterable[DataFormat]:
#         return self._registry.values()
#
#     def __getitem__(self, item: str) -> DataFormat:
#         return self._registry[item]
#
#
# data_format_registry = DataFormatRegistry()
# core_data_formats_precedence = [
#     # Roughly ordered from most universal / "default" to least
#     # Ordering used when inferring DataFormat from raw object and have ambiguous object (eg an empty list)
#     RecordsListFormat,
#     DataFrameFormat,
#     DatabaseCursorFormat,
#     DatabaseTableRefFormat,
#     RecordsListGeneratorFormat,
#     DataFrameGeneratorFormat,
#     DelimitedFilePointerFormat,
# ]
# for fmt in core_data_formats_precedence:
#     data_format_registry.register(fmt)
#
#
# def get_data_format_of_object(obj: Any) -> Optional[DataFormat]:
#     maybes = []
#     for m in data_format_registry.all():
#         if m.maybe_instance(obj):
#             maybes.append(m)
#     if len(maybes) == 1:
#         return maybes[0]
#     elif len(maybes) > 1:
#         return [f for f in core_data_formats_precedence if f in maybes][0]
#     return None
#
#
# def get_records_list_sample(
#     obj: Union[pd.DataFrame, RecordsList, RecordsListGenerator, DataFrameGenerator]
# ) -> Optional[RecordsList]:
#     if isinstance(obj, list):
#         return obj
#     if isinstance(obj, pd.DataFrame):
#         return obj.to_dict(orient="records")
#     if isinstance(obj, DataFrameGenerator):
#         return get_records_list_sample(obj.get_one())
#     if isinstance(obj, RecordsListGenerator):
#         return obj.get_one()
#     if isinstance(obj, collections.abc.Generator):
#         raise TypeError("Generators must be `tee`d before being passed in")
#     raise TypeError(obj)
