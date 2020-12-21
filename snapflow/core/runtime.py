from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Type

from snapflow.core.environment import Environment
from snapflow.storage.storage import (
    DatabaseStorageClass,
    LocalPythonStorageEngine,
    MysqlStorageEngine,
    PostgresStorageEngine,
    PythonStorageClass,
    SqliteStorageEngine,
    Storage,
    StorageApi,
    StorageClass,
    StorageEngine,
)
from snapflow.utils.common import rand_str
from snapflow.utils.registry import global_registry

if TYPE_CHECKING:
    from snapflow.storage.db.api import DatabaseApi


class RuntimeClass:
    natural_storage_class: Type[StorageClass]


class DatabaseRuntimeClass(RuntimeClass):
    natural_storage_class = DatabaseStorageClass


class PythonRuntimeClass(RuntimeClass):
    natural_storage_class = PythonStorageClass


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
    runtime_engine: Type[RuntimeEngine]
    configuration: Optional[Dict] = None

    @classmethod
    def from_storage(cls, storage: Storage) -> Runtime:
        for eng in global_registry.all(RuntimeEngine):
            if eng.natural_storage_engine == storage.storage_engine:
                return Runtime(storage.url, eng)
        raise NotImplementedError(f"No matching runtime {storage}")

    def as_storage(self) -> Storage:
        return Storage(
            url=self.url,
            storage_engine=self.runtime_engine.natural_storage_engine,
        )

    # def get_default_local_storage(self):
    #     try:
    #         return self.as_storage()
    #     except KeyError:
    #         return Runtime(  # type: ignore
    #             url=f"memory://_runtime_default_{rand_str(6)}",
    #             storage_class=RuntimeClass.MEMORY,
    #             storage_engine=RuntimeEngine.DICT,
    #         )

    def get_api(self) -> StorageApi:
        # TODO: separate runtime apis eventually
        return self.as_storage().get_api()
