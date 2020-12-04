from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Dict, Optional

from snapflow.core.environment import Environment
from snapflow.core.storage.storage import Storage, StorageClass, StorageEngine
from snapflow.utils.common import rand_str

if TYPE_CHECKING:
    from snapflow.db.api import DatabaseAPI


class RuntimeClass(Enum):
    PYTHON = "python"
    DATABASE = "database"
    # R = "r" # TODO


class RuntimeEngine(Enum):
    # Python
    LOCAL = "local"
    # RDBMS
    POSTGRES = "postgres"
    MYSQL = "mysql"  # TODO
    SQLITE = "sqlite"


class RuntimeType(Enum):
    LOCAL_PYTHON = (RuntimeClass.PYTHON, RuntimeEngine.LOCAL)
    POSTGRES_DATABASE = (RuntimeClass.DATABASE, RuntimeEngine.POSTGRES)
    MYSQL_DATABASE = (RuntimeClass.DATABASE, RuntimeEngine.MYSQL)


runtime_storage_dual_mapping = {
    # Storages
    StorageClass.DATABASE: RuntimeClass.DATABASE,
    StorageEngine.POSTGRES: RuntimeEngine.POSTGRES,
    StorageEngine.MYSQL: RuntimeEngine.MYSQL,
    StorageEngine.SQLITE: RuntimeEngine.SQLITE,
    # Runtimes
    RuntimeClass.DATABASE: StorageClass.DATABASE,
    RuntimeEngine.POSTGRES: StorageEngine.POSTGRES,
    RuntimeEngine.MYSQL: StorageEngine.MYSQL,
    RuntimeEngine.SQLITE: StorageEngine.SQLITE,
}


@dataclass(frozen=True)
class Runtime:
    url: str
    runtime_class: RuntimeClass
    runtime_engine: RuntimeEngine
    configuration: Optional[Dict] = None

    @classmethod
    def from_storage(cls, storage: Storage) -> Runtime:
        if storage.storage_class not in runtime_storage_dual_mapping:
            raise ValueError(f"Storage {storage} cannot be adapted to a Runtime")
        return Runtime(
            url=storage.url,
            runtime_class=runtime_storage_dual_mapping[storage.storage_class],  # type: ignore # TODO
            runtime_engine=runtime_storage_dual_mapping[storage.storage_engine],  # type: ignore # TODO
        )

    def as_storage(self):
        if self.runtime_class not in runtime_storage_dual_mapping:
            raise ValueError(f"Runtime {self} cannot be adapted to a Storage")
        return Storage(
            url=self.url,
            storage_class=runtime_storage_dual_mapping[self.runtime_class],  # type: ignore # TODO
            storage_engine=runtime_storage_dual_mapping[self.runtime_engine],  # type: ignore # TODO
        )

    # def get_default_local_storage(self):
    #     try:
    #         return self.as_storage()
    #     except KeyError:
    #         return Storage(  # type: ignore
    #             url=f"memory://_runtime_default_{rand_str(6)}",
    #             storage_class=StorageClass.MEMORY,
    #             storage_engine=StorageEngine.DICT,
    #         )

    def get_database_api(self, env: Environment) -> DatabaseAPI:
        from snapflow.db.api import get_database_api_class

        db_api_cls = get_database_api_class(self.as_storage().storage_engine)
        return db_api_cls(env, self.url)
