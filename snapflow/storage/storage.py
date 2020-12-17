from __future__ import annotations

import enum
import os
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type
from urllib.parse import urlparse

from loguru import logger
from snapflow.storage.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFormat,
    DataFrameFormat,
    DataFrameIteratorFormat,
    DelimitedFileFormat,
    DelimitedFileObjectFormat,
    JsonLinesFileFormat,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.data_formats.base import FileDataFormatBase, MemoryDataFormatBase
from snapflow.storage.data_records import MemoryDataRecords
from snapflow.utils.common import cf, rand_str
from snapflow.utils.registry import ClassBasedEnum, global_registry

if TYPE_CHECKING:
    pass


class StorageClass(ClassBasedEnum):
    natural_format: DataFormat
    supported_formats: List[DataFormat] = []

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        raise NotImplementedError


class DatabaseStorageClass(StorageClass):
    natural_format = DatabaseTableFormat
    supported_formats = [DatabaseTableFormat]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from snapflow.storage.db.api import DatabaseStorageApi

        return DatabaseStorageApi


class PythonStorageClass(StorageClass):
    natural_format = RecordsFormat
    supported_formats = [MemoryDataFormatBase]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        return PythonStorageApi


class FileSystemStorageClass(StorageClass):
    natural_format = DelimitedFileFormat
    supported_formats = [FileDataFormatBase]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from snapflow.storage.file_system import FileSystemStorageApi

        return FileSystemStorageApi


class StorageEngine(ClassBasedEnum):
    storage_class: Type[StorageClass]
    schemes: List[str] = []

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        return cls.storage_class.get_api_cls()

    @classmethod
    def get_supported_formats(cls) -> List[DataFormat]:
        return cls.storage_class.supported_formats

    @classmethod
    def is_supported_format(cls, fmt: DataFormat) -> bool:
        for sfmt in cls.get_supported_formats():
            if issubclass(fmt, sfmt):
                return True
        return False

    @classmethod
    def get_natural_format(cls) -> DataFormat:
        return cls.storage_class.natural_format


class SqliteStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["sqlite"]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from snapflow.storage.db.sqlite import SqliteDatabaseStorageApi

        return SqliteDatabaseStorageApi


class PostgresStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["postgres", "postgresql"]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from snapflow.storage.db.postgres import PostgresDatabaseStorageApi

        return PostgresDatabaseStorageApi


class MysqlStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["mysql"]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from snapflow.storage.db.mysql import MysqlDatabaseStorageApi

        return MysqlDatabaseStorageApi


class LocalFileSystemStorageEngine(StorageEngine):
    storage_class = FileSystemStorageClass
    schemes = ["file"]


class LocalPythonStorageEngine(StorageEngine):
    storage_class = PythonStorageClass
    schemes = ["python"]


core_storage_engines: List[Type[StorageEngine]] = [
    SqliteStorageEngine,
    PostgresStorageEngine,
    MysqlStorageEngine,
    LocalFileSystemStorageEngine,
    LocalPythonStorageEngine,
]

for eng in core_storage_engines:
    global_registry.register(eng)


def register_storage_engine(eng: Type[StorageEngine]):
    global_registry.register(eng)


def get_engine_for_scheme(scheme: str) -> Type[StorageEngine]:
    # Take first match IN REVERSE ORDER they were added
    # (so an Engine added later - by user perhaps - takes precedence)
    for eng in list(global_registry.all(StorageEngine))[::-1]:
        if scheme in eng.schemes:
            return eng
    raise Exception(f"No matching engine for scheme {scheme}")  # TODO


@dataclass(frozen=True)
class Storage:
    url: str
    storage_engine: Type[StorageEngine]

    @classmethod
    def from_url(cls, url: str) -> Storage:
        parsed = urlparse(url)
        engine = get_engine_for_scheme(parsed.scheme)
        return Storage(url=url, storage_engine=engine)

    def get_api(self) -> StorageApi:
        return self.storage_engine.get_api_cls()(self)


class NameDoesNotExistError(Exception):
    pass


class StorageApi:
    def __init__(self, storage: Storage):
        self.storage = storage

    def exists(self, name: str) -> bool:
        raise NotImplementedError

    def record_count(self, name: str) -> Optional[int]:
        raise NotImplementedError

    def copy(self, name: str, to_name: str):
        raise NotImplementedError

    def create_alias(
        self, name: str, alias: str
    ):  # TODO: rename to overwrite_alias or set_alias?
        raise NotImplementedError


LOCAL_PYTHON_STORAGE: Dict[str, MemoryDataRecords] = {}  # TODO: global state...


def new_local_python_storage() -> Storage:
    return Storage.from_url(f"python://{rand_str(10)}/")


def clear_local_storage():
    LOCAL_PYTHON_STORAGE.clear()


class PythonStorageApi(StorageApi):
    def get_path(self, name: str) -> str:
        return os.path.join(self.storage.url, name)

    def get(self, name: str) -> MemoryDataRecords:
        pth = self.get_path(name)
        mdr = LOCAL_PYTHON_STORAGE.get(pth)
        if mdr is None:
            raise NameDoesNotExistError(name)
        return mdr

    def remove(self, name: str):
        pth = self.get_path(name)
        del LOCAL_PYTHON_STORAGE[pth]

    def put(self, name: str, mdr: MemoryDataRecords):
        pth = self.get_path(name)
        LOCAL_PYTHON_STORAGE[pth] = mdr

    def exists(self, name: str) -> bool:
        pth = self.get_path(name)
        return pth in LOCAL_PYTHON_STORAGE

    def record_count(self, name: str) -> Optional[int]:
        mdr = self.get(name)
        return mdr.record_count

    def copy(self, name: str, to_name: str):
        mdr = self.get(name)
        mdr_copy = deepcopy(mdr)
        self.put(to_name, mdr_copy)

    def create_alias(self, name: str, alias: str):
        mdr = self.get(name)
        self.put(alias, mdr)
