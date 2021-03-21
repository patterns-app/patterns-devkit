from __future__ import annotations

import tempfile

import pyarrow as pa
from snapflow.storage.data_copy.base import Conversion, StorageFormat
from snapflow.storage.data_copy.file_to_memory import (
    copy_delim_file_to_records,
    copy_json_file_to_arrow,
)
from snapflow.storage.data_formats import DelimitedFileFormat, RecordsFormat
from snapflow.storage.data_formats.arrow_table import ArrowTableFormat
from snapflow.storage.data_formats.json_lines_file import JsonLinesFileFormat
from snapflow.storage.file_system import FileSystemStorageApi
from snapflow.storage.storage import (
    LocalPythonStorageEngine,
    PythonStorageApi,
    Storage,
    new_local_python_storage,
)
from tests.utils import TestSchema4


def test_file_to_mem():
    dr = tempfile.gettempdir()
    s: Storage = Storage.from_url(f"file://{dr}")
    fs_api: FileSystemStorageApi = s.get_api()
    mem_api: PythonStorageApi = new_local_python_storage().get_api()
    name = "_test"
    fs_api.write_lines_to_file(name, ["f1,f2", "hi,2"])
    # Records
    records_obj = [{"f1": "hi", "f2": 2}]
    conversion = Conversion(
        StorageFormat(s.storage_engine, DelimitedFileFormat),
        StorageFormat(LocalPythonStorageEngine, RecordsFormat),
    )
    copy_delim_file_to_records.copy(
        name, name, conversion, fs_api, mem_api, schema=TestSchema4
    )
    assert mem_api.get(name).records_object == records_obj

    # Json lines
    name = "_json_test"
    fs_api.write_lines_to_file(name, ['{"f1":"hi","f2":2}'])
    conversion = Conversion(
        StorageFormat(s.storage_engine, JsonLinesFileFormat),
        StorageFormat(LocalPythonStorageEngine, ArrowTableFormat),
    )
    copy_json_file_to_arrow.copy(
        name, name, conversion, fs_api, mem_api, schema=TestSchema4
    )
    expected = pa.Table.from_pydict({"f1": ["hi"], "f2": [2]})
    assert mem_api.get(name).records_object == expected
