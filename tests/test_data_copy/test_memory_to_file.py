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
from snapflow.storage.data_copy.database_to_memory import copy_db_to_records
from snapflow.storage.data_copy.memory_to_database import copy_records_to_db
from snapflow.storage.data_copy.memory_to_file import (
    copy_file_object_to_delim_file,
    copy_records_to_delim_file,
)
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
from snapflow.storage.file_system import FileSystemStorageApi
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
from snapflow.utils.data import read_csv
from tests.utils import TestSchema1, TestSchema4


def test_records_to_file():
    dr = tempfile.gettempdir()
    s: Storage = Storage.from_url(f"file://{dr}")
    fs_api: FileSystemStorageApi = s.get_api()
    mem_api: PythonStorageApi = new_local_python_storage().get_api()
    name = "_test"
    fmt = RecordsFormat
    obj = [{"f1": "hi", "f2": 2}]
    mdr = as_records(obj, data_format=fmt)
    mem_api.put(name, mdr)
    conversion = Conversion(
        StorageFormat(LocalPythonStorageEngine, fmt),
        StorageFormat(s.storage_engine, DelimitedFileFormat),
    )
    copy_records_to_delim_file.copy(
        name, name, conversion, mem_api, fs_api, schema=TestSchema4
    )
    with fs_api.open(name) as f:
        recs = list(read_csv(f))
        recs = RecordsFormat.conform_records_to_schema(recs, TestSchema4)
        assert recs == obj


def test_obj_to_file():
    dr = tempfile.gettempdir()
    s: Storage = Storage.from_url(f"file://{dr}")
    fs_api: FileSystemStorageApi = s.get_api()
    mem_api: PythonStorageApi = new_local_python_storage().get_api()
    name = "_test"
    fmt = DelimitedFileObjectFormat
    obj = (lambda: StringIO("f1,f2\nhi,2"),)[0]
    mdr = as_records(obj(), data_format=fmt)
    mem_api.put(name, mdr)
    conversion = Conversion(
        StorageFormat(LocalPythonStorageEngine, fmt),
        StorageFormat(s.storage_engine, DelimitedFileFormat),
    )
    copy_file_object_to_delim_file.copy(
        name, name, conversion, mem_api, fs_api, schema=TestSchema4
    )
    with fs_api.open(name) as f:
        assert f.read() == obj().read()
