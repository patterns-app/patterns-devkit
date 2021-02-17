from __future__ import annotations

import tempfile
import types
from io import StringIO
from typing import Optional, Type

import pytest
from snapflow.core.data_block import DataBlockMetadata, create_data_block_from_records
from snapflow.storage.data_copy.base import (
    Conversion,
    DataCopier,
    NetworkToMemoryCost,
    NoOpCost,
    StorageFormat,
    datacopy,
    get_datacopy_lookup,
)
from snapflow.storage.data_copy.database_to_memory import (
    copy_db_to_cursor,
    copy_db_to_records,
    copy_db_to_records_iterator,
)
from snapflow.storage.data_copy.memory_to_database import copy_records_to_db
from snapflow.storage.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFrameFormat,
    DelimitedFileFormat,
    JsonLinesFileFormat,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.data_formats.data_frame import DataFrameIteratorFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
)
from snapflow.storage.data_records import MemoryDataRecords, as_records
from snapflow.storage.db.api import DatabaseApi, DatabaseStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    LocalPythonStorageEngine,
    PostgresStorageEngine,
    PythonStorageApi,
    PythonStorageClass,
    Storage,
    clear_local_storage,
    new_local_python_storage,
)
from tests.utils import TestSchema1, TestSchema4


@pytest.mark.parametrize(
    "url",
    [
        "sqlite://",
        "postgresql://localhost",
        "mysql://",
    ],
)
def test_db_to_mem(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    mem_api: PythonStorageApi = new_local_python_storage().get_api()
    if not s.get_api().dialect_is_supported():
        return
    with api_cls.temp_local_database() as db_url:
        api: DatabaseStorageApi = Storage.from_url(db_url).get_api()
        name = "_test"
        api.execute_sql(f"create table {name} as select 1 a, 2 b")
        # Records
        conversion = Conversion(
            StorageFormat(s.storage_engine, DatabaseTableFormat),
            StorageFormat(LocalPythonStorageEngine, RecordsFormat),
        )
        copy_db_to_records.copy(name, name, conversion, api, mem_api)
        assert mem_api.get(name).records_object == [{"a": 1, "b": 2}]
        # Records iterator
        conversion = Conversion(
            StorageFormat(s.storage_engine, DatabaseTableFormat),
            StorageFormat(LocalPythonStorageEngine, RecordsIteratorFormat),
        )
        try:
            copy_db_to_records_iterator.copy(name, name, conversion, api, mem_api)
            assert list(mem_api.get(name).records_object) == [[{"a": 1, "b": 2}]]
        finally:
            mem_api.get(name).closeable()
        # DatabaseCursor
        conversion = Conversion(
            StorageFormat(s.storage_engine, DatabaseTableFormat),
            StorageFormat(LocalPythonStorageEngine, DatabaseCursorFormat),
        )
        try:
            copy_db_to_cursor.copy(name, name, conversion, api, mem_api)
            assert list(mem_api.get(name).records_object) == [(1, 2)]
        finally:
            mem_api.get(name).closeable()
