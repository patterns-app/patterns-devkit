from __future__ import annotations

import os
import tempfile
from typing import Type

import pytest
from snapflow.storage.data_records import MemoryDataRecords, as_records
from snapflow.storage.db.api import DatabaseApi, DatabaseStorageApi
from snapflow.storage.db.mysql import MysqlDatabaseStorageApi
from snapflow.storage.db.postgres import PostgresDatabaseStorageApi
from snapflow.storage.file_system import FileSystemStorageApi
from snapflow.storage.storage import (
    LOCAL_PYTHON_STORAGE,
    LocalFileSystemStorageEngine,
    LocalPythonStorageEngine,
    MysqlStorageEngine,
    PostgresStorageEngine,
    PythonStorageApi,
    SqliteStorageEngine,
    Storage,
)


def test_storage():
    s = Storage.from_url("sqlite://")
    assert s.storage_engine is SqliteStorageEngine
    s = Storage.from_url("postgres://localhost")
    assert s.storage_engine is PostgresStorageEngine
    s = Storage.from_url("mysql://localhost")
    assert s.storage_engine is MysqlStorageEngine
    s = Storage.from_url("file:///")
    assert s.storage_engine is LocalFileSystemStorageEngine
    s = Storage.from_url("python://")
    assert s.storage_engine is LocalPythonStorageEngine


def test_storage_api():
    s = Storage.from_url("sqlite://").get_api()
    assert isinstance(s, DatabaseStorageApi)
    s = Storage.from_url("postgres://localhost").get_api()
    assert isinstance(s, PostgresDatabaseStorageApi)
    s = Storage.from_url("mysql://localhost").get_api()
    assert isinstance(s, MysqlDatabaseStorageApi)
    s = Storage.from_url("file:///").get_api()
    assert isinstance(s, FileSystemStorageApi)
    s = Storage.from_url("python://").get_api()
    assert isinstance(s, PythonStorageApi)


@pytest.mark.parametrize(
    "url",
    [
        "sqlite://",
        "postgresql://localhost",
        "mysql://",
    ],
)
def test_database_api_core_operations(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    if not s.get_api().dialect_is_supported():
        return
    with api_cls.temp_local_database() as db_url:
        api: DatabaseApi = Storage.from_url(db_url).get_api()
        name = "_test"
        api.execute_sql(f"create table {name} as select 1 a, 2 b")
        assert api.exists(name)
        assert not api.exists(name + "doesntexist")
        assert api.record_count(name) == 1
        api.create_alias(name, name + "alias")
        assert api.record_count(name + "alias") == 1
        api.copy(name, name + "copy")
        assert api.record_count(name + "copy") == 1


@pytest.mark.parametrize(
    "url",
    [
        f"file://{tempfile.mkdtemp()}",
    ],
)
def test_filesystem_api_core_operations(url):
    api: FileSystemStorageApi = Storage.from_url(url).get_api()
    name = "_test"
    pth = os.path.join(url[7:], name)
    with open(pth, "w") as f:
        f.writelines(["f1,f2\n", "1,2\n"])
    assert api.exists(name)
    assert not api.exists(name + "doesntexist")
    assert api.record_count(name) == 2
    api.create_alias(name, name + "alias")
    assert api.record_count(name + "alias") == 2
    api.copy(name, name + "copy")
    assert api.record_count(name + "copy") == 2


@pytest.mark.parametrize(
    "url",
    [
        "python://",
    ],
)
def test_filesystem_api_core_operations(url):
    api: PythonStorageApi = Storage.from_url(url).get_api()
    name = "_test"
    api.put(name, as_records([{"a": 1}, {"b": 2}]))
    assert api.exists(name)
    assert not api.exists(name + "doesntexist")
    assert api.record_count(name) == 2
    api.create_alias(name, name + "alias")
    assert api.record_count(name + "alias") == 2
    api.copy(name, name + "copy")
    assert api.record_count(name + "copy") == 2
