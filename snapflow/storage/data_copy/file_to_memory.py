from __future__ import annotations

from pyarrow import json as pa_json
from snapflow.core.typing.inference import conform_records_to_schema
from snapflow.schema.base import Schema
from snapflow.storage.data_copy.base import (
    Conversion,
    DiskToBufferCost,
    DiskToMemoryCost,
    FormatConversionCost,
    NetworkToBufferCost,
    NetworkToMemoryCost,
    NoOpCost,
    datacopy,
)
from snapflow.storage.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRef,
    DatabaseTableRefFormat,
    RecordsFormat,
)
from snapflow.storage.data_formats.arrow_table import ArrowTableFormat
from snapflow.storage.data_formats.delimited_file import DelimitedFileFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
)
from snapflow.storage.data_formats.json_lines_file import JsonLinesFileFormat
from snapflow.storage.data_records import as_records
from snapflow.storage.db.api import DatabaseStorageApi
from snapflow.storage.db.utils import result_proxy_to_records
from snapflow.storage.file_system import FileSystemStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    PythonStorageApi,
    PythonStorageClass,
    StorageApi,
)
from snapflow.utils.data import read_csv


@datacopy(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[DelimitedFileFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsFormat],
    cost=DiskToMemoryCost + FormatConversionCost,
)
def copy_delim_file_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, FileSystemStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    with from_storage_api.open(from_name) as f:
        records = list(read_csv(f.readlines()))
        mdr = as_records(records, data_format=RecordsFormat, schema=schema)
        mdr = mdr.conform_to_schema()
        to_storage_api.put(to_name, mdr)


@datacopy(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[DelimitedFileFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[DelimitedFileObjectFormat],
    cost=DiskToBufferCost,
)
def copy_delim_file_to_file_object(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, FileSystemStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    with from_storage_api.open(from_name) as f:
        mdr = as_records(f, data_format=DelimitedFileObjectFormat, schema=schema)
        mdr = mdr.conform_to_schema()
        to_storage_api.put(to_name, mdr)


@datacopy(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[JsonLinesFileFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[ArrowTableFormat],
    cost=DiskToMemoryCost,  # TODO: conversion cost might be minimal cuz in C?
)
def copy_json_file_to_arrow(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, FileSystemStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    pth = from_storage_api.get_path(from_name)
    at = pa_json.read_json(pth)
    mdr = as_records(at, data_format=ArrowTableFormat, schema=schema)
    mdr = mdr.conform_to_schema()
    to_storage_api.put(to_name, mdr)
