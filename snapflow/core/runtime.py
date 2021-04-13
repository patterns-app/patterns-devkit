from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Type, Union
from urllib.parse import urlparse

from dcp.storage.base import (
    DatabaseStorageClass,
    LocalPythonStorageEngine,
    MemoryStorageClass,
    MysqlStorageEngine,
    PostgresStorageEngine,
    SqliteStorageEngine,
    Storage,
    StorageApi,
    StorageClass,
    StorageEngine,
)
from dcp.utils.common import rand_str
from snapflow.core.environment import Environment
from snapflow.utils.registry import global_registry


class RuntimeClass:
    natural_storage_class: Type[StorageClass]


class DatabaseRuntimeClass(RuntimeClass):
    natural_storage_class = DatabaseStorageClass


class PythonRuntimeClass(RuntimeClass):
    natural_storage_class = MemoryStorageClass


class RuntimeEngine:
    runtime_class: Type[RuntimeClass]
    natural_storage_engine: Type[StorageEngine]
    schemes: List[str]


class SqliteRuntimeEngine(RuntimeEngine):
    runtime_class = DatabaseRuntimeClass
    natural_storage_engine = SqliteStorageEngine
    schemes = ["sqlite"]


class PostgresRuntimeEngine(RuntimeEngine):
    runtime_class = DatabaseRuntimeClass
    natural_storage_engine = PostgresStorageEngine
    schemes = ["postgres", "postgresql"]


class MysqlRuntimeEngine(RuntimeEngine):
    runtime_class = DatabaseRuntimeClass
    natural_storage_engine = MysqlStorageEngine
    schemes = ["mysql"]


class LocalPythonRuntimeEngine(RuntimeEngine):
    runtime_class = PythonRuntimeClass
    natural_storage_engine = LocalPythonStorageEngine
    schemes = ["python"]


core_runtime_engines: List[Type[RuntimeEngine]] = [
    SqliteRuntimeEngine,
    PostgresRuntimeEngine,
    MysqlRuntimeEngine,
    LocalPythonRuntimeEngine,
]

for eng in core_runtime_engines:
    global_registry.register(eng)


def register_runtime_engine(eng: Type[RuntimeEngine]):
    global_registry.register(eng)


def get_engine_for_scheme(scheme: str) -> Type[RuntimeEngine]:
    # Take first match IN REVERSE ORDER they were added
    # (so an Engine added later - by user perhaps - takes precedence)
    for eng in list(global_registry.all(RuntimeEngine))[::-1]:
        if scheme in eng.schemes:
            return eng
    raise Exception(f"No matching engine for scheme {scheme}")  # TODO


@dataclass(frozen=True)
class Runtime:
    url: str
    configuration: Optional[Dict] = None

    @classmethod
    def from_storage(cls, storage: Storage) -> Runtime:
        for eng in global_registry.all(RuntimeEngine):
            if eng.natural_storage_engine == storage.storage_engine:
                return Runtime(storage.url)
        raise NotImplementedError(f"No matching runtime {storage}")

    @classmethod
    def from_url(cls, url: str) -> Runtime:
        return Runtime(url=url)

    def as_storage(self) -> Storage:
        return Storage(url=self.url)

    @property
    def runtime_engine(self) -> Type[RuntimeEngine]:
        parsed = urlparse(self.url)
        return get_engine_for_scheme(parsed.scheme)

    def get_api(self) -> StorageApi:
        # TODO: separate runtime apis eventually
        return self.as_storage().get_api()


def ensure_runtime(s: Union[Runtime, str]) -> Runtime:
    if isinstance(s, str):
        s = Runtime.from_url(s)
    return s
