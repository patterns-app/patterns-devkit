from __future__ import annotations

import enum
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Type
from urllib.parse import urlparse

from basis.core.data_format import DataFormat
from basis.core.data_resource import LocalMemoryDataRecords, StoredDataResourceMetadata
from basis.core.environment import Environment
from basis.utils.common import cf, printd, rand_str

if TYPE_CHECKING:
    from basis.db.api import DatabaseAPI


class StorageClass(enum.Enum):
    DATABASE = "database"
    FILE_SYSTEM = "file_system"
    MEMORY = "memory"


class StorageEngine(enum.Enum):
    # RDBMS
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    # FILE_SYSTEM
    LOCAL = "local"
    # MEMORY
    DICT = "dict"


class StorageType(enum.Enum):
    POSTGRES_DATABASE = (StorageClass.DATABASE, StorageEngine.POSTGRES)
    MYSQL_DATABASE = (StorageClass.DATABASE, StorageEngine.MYSQL)
    SQLITE_DATABASE = (StorageClass.DATABASE, StorageEngine.SQLITE)
    LOCAL_FILE_SYSTEM = (StorageClass.FILE_SYSTEM, StorageEngine.LOCAL)
    DICT_MEMORY = (StorageClass.MEMORY, StorageEngine.DICT)

    def display(self) -> str:
        return f"{self.value[0].value}-{self.value[1].value}"


NATURAL_STORAGE_FORMAT = {
    StorageClass.MEMORY: DataFormat.DICT_LIST,
    StorageClass.DATABASE: DataFormat.DATABASE_TABLE,
    StorageClass.FILE_SYSTEM: DataFormat.DELIMITED_FILE,
}

NATURAL_STORAGE_CLASS = {
    DataFormat.DICT_LIST: StorageClass.MEMORY,
    DataFormat.DATAFRAME: StorageClass.MEMORY,
    DataFormat.DATABASE_TABLE_REF: StorageClass.MEMORY,
    DataFormat.DATABASE_CURSOR: StorageClass.MEMORY,
    DataFormat.JSON_LIST: StorageClass.MEMORY,
    DataFormat.DELIMITED_FILE_POINTER: StorageClass.MEMORY,
    DataFormat.DATABASE_TABLE: StorageClass.DATABASE,
    DataFormat.DELIMITED_FILE: StorageClass.FILE_SYSTEM,
    DataFormat.JSON_LIST_FILE: StorageClass.FILE_SYSTEM,
    DataFormat.DICT_LIST_ITERATOR: StorageClass.MEMORY,
    DataFormat.DATAFRAME_ITERATOR: StorageClass.MEMORY,
}


@dataclass(frozen=True)
class StorageResource:
    url: str
    storage_class: StorageClass
    storage_engine: StorageEngine

    @classmethod
    def from_url(cls, url: str) -> StorageResource:
        parsed = urlparse(url)
        scheme_to_type = {
            "file": StorageType.LOCAL_FILE_SYSTEM,
            "postgres": StorageType.POSTGRES_DATABASE,
            "mysql": StorageType.MYSQL_DATABASE,
            "memory": StorageType.DICT_MEMORY,
            "sqlite": StorageType.SQLITE_DATABASE,
        }
        class_, engine = scheme_to_type[parsed.scheme].value
        return StorageResource(url=url, storage_class=class_, storage_engine=engine)

    @property
    def storage_type(self) -> StorageType:
        for t in StorageType:
            if t.value == (self.storage_class, self.storage_engine):
                return t
        raise Exception(f"No storage type defined for engine and class {self}")

    @property
    def natural_storage_format(self) -> DataFormat:
        return NATURAL_STORAGE_FORMAT[self.storage_class]

    def get_database_api(self, env: Environment) -> DatabaseAPI:
        from basis.db.api import get_database_api_class

        db_api_cls = get_database_api_class(self.storage_engine)
        return db_api_cls(env, self)

    def get_manager(self, env: Environment) -> StorageManager:
        return manager_lookup[self.storage_class](env, self)


class StorageManager:
    def __init__(self, env: Environment, storage_resource: StorageResource):
        self.env = env
        self.storage_resource = storage_resource

    def exists(self, stored_data_resource: StoredDataResourceMetadata) -> bool:
        raise NotImplementedError

    def record_count(
        self, stored_data_resource: StoredDataResourceMetadata
    ) -> Optional[int]:
        raise NotImplementedError


class MemoryStorageManager(StorageManager):
    @property
    def storage(self) -> LocalMemoryStorage:
        return LocalMemoryStorage(self.env, self.storage_resource)

    def exists(self, stored_data_resource: StoredDataResourceMetadata) -> bool:
        return self.storage.exists(stored_data_resource)

    def record_count(
        self, stored_data_resource: StoredDataResourceMetadata
    ) -> Optional[int]:
        if not self.exists(stored_data_resource):
            return None
        return self.storage.get_local_memory_data_records(
            stored_data_resource
        ).record_count


class DatabaseStorageManager(StorageManager):
    @property
    def database(self) -> DatabaseAPI:
        return self.storage_resource.get_database_api(self.env)

    def exists(self, stored_data_resource: StoredDataResourceMetadata) -> bool:
        return self.database.exists(stored_data_resource.get_name(self.env))

    def record_count(
        self, stored_data_resource: StoredDataResourceMetadata
    ) -> Optional[int]:
        if not self.exists(stored_data_resource):
            return None
        return self.database.count(stored_data_resource.get_name(self.env))


manager_lookup: Dict[StorageClass, Type[StorageManager]] = {
    StorageClass.MEMORY: MemoryStorageManager,
    StorageClass.DATABASE: DatabaseStorageManager,
}


class Storage:
    def __init__(self, env: Environment, storage_resource: StorageResource):
        self.env = env
        self.storage_resource = storage_resource

    def exists(self, stored_data_resource: StoredDataResourceMetadata) -> bool:
        return self._exists(stored_data_resource)

    def _exists(self, stored_data_resource: StoredDataResourceMetadata) -> bool:
        raise NotImplementedError

    def get_local_memory_data_records(
        self, stored_data_resource: StoredDataResourceMetadata
    ) -> LocalMemoryDataRecords:
        ldr = self._get(stored_data_resource)
        printd(
            f"← Getting {cf.bold(ldr.record_count_display)} records of SDR#{cf.bold(stored_data_resource.id)} in {self.storage_resource}"
        )
        return ldr

    def store_local_memory_data_records(
        self,
        stored_data_resource: StoredDataResourceMetadata,
        data_records: LocalMemoryDataRecords,
    ):
        if self.exists(stored_data_resource):
            raise Exception("SDRs are immutable")  # TODO / cleanup
        printd(
            f"➞ Putting {cf.bold(data_records.record_count)} records of SDR#{cf.bold(stored_data_resource.id)} in {self.storage_resource}"
        )
        data_records.validate_and_conform_otype(
            stored_data_resource.get_otype(self.env)
        )
        self._put(stored_data_resource, data_records)

    def _get(
        self, stored_data_resource: StoredDataResourceMetadata
    ) -> LocalMemoryDataRecords:
        raise NotImplementedError

    def _put(
        self,
        stored_data_resource: StoredDataResourceMetadata,
        data_records: LocalMemoryDataRecords,
    ):
        raise NotImplementedError


global_memory_storage: Dict[str, Any] = {}


class LocalMemoryStorage(Storage):
    def get_url(self, stored_data_resource: StoredDataResourceMetadata) -> str:
        name = stored_data_resource.get_name(self.env)
        return os.path.join(self.storage_resource.url, name)

    def get_key(self, stored_data_resource: StoredDataResourceMetadata) -> str:
        return self.get_url(stored_data_resource)

    def _put(
        self,
        stored_data_resource: StoredDataResourceMetadata,
        data_records: LocalMemoryDataRecords,
    ):
        if data_records.records_object is None:
            raise
        key = self.get_key(stored_data_resource)
        global_memory_storage[key] = data_records

    def _get(
        self, stored_data_resource: StoredDataResourceMetadata
    ) -> LocalMemoryDataRecords:
        key = self.get_key(stored_data_resource)
        ldr = global_memory_storage[key]
        return (
            ldr.copy()
        )  # IMPORTANT: It's critical that we *copy* here, otherwise user may mutate an SDR/DR -- absolute no no
        # TODO: should also copy on put? Just to be safe. Copying is not zero cost of course...

    def _exists(self, stored_data_resource: StoredDataResourceMetadata) -> bool:
        return self.get_key(stored_data_resource) in global_memory_storage


def new_local_memory_storage():
    local_storage = StorageResource(
        url=f"memory://_runtime_default_{rand_str(6)}",
        storage_class=StorageClass.MEMORY,
        storage_engine=StorageEngine.DICT,
    )
    return local_storage
