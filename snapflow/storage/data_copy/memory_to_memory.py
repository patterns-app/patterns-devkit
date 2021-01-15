from typing import Sequence

import pandas as pd
from snapflow.schema.base import Schema
from snapflow.storage.data_copy.base import (
    BufferToBufferCost,
    Conversion,
    DiskToMemoryCost,
    MemoryToBufferCost,
    MemoryToMemoryCost,
    datacopy,
)
from snapflow.storage.data_formats import (
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFormat,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.data_formats.data_frame import (
    DataFrameFormat,
    DataFrameIterator,
    DataFrameIteratorFormat,
)
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
)
from snapflow.storage.data_records import as_records
from snapflow.storage.db.api import DatabaseStorageApi
from snapflow.storage.file_system import FileSystemStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    PythonStorageApi,
    PythonStorageClass,
    StorageApi,
)
from snapflow.utils.data import (
    SampleableIterator,
    iterate_chunks,
    read_csv,
    with_header,
    write_csv,
)
from snapflow.utils.pandas import dataframe_to_records, records_to_dataframe


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[DataFrameFormat],
    cost=MemoryToMemoryCost,
)
def copy_records_to_df(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    df = records_to_dataframe(mdr.records_object, schema)
    to_mdr = as_records(df, data_format=DataFrameFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[DataFrameFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsFormat],
    cost=MemoryToMemoryCost,
)
def copy_df_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    df = dataframe_to_records(mdr.records_object, schema)
    to_mdr = as_records(df, data_format=RecordsFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[DataFrameIteratorFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsIteratorFormat],
    cost=BufferToBufferCost,
)
def copy_df_iterator_to_records_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    itr = (dataframe_to_records(df, schema) for df in mdr.records_object)
    to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsIteratorFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[DataFrameIteratorFormat],
    cost=BufferToBufferCost,
)
def copy_records_iterator_to_df_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    itr = (pd.DataFrame(records) for records in mdr.records_object)
    to_mdr = as_records(itr, data_format=DataFrameIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsIteratorFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsFormat],
    cost=MemoryToMemoryCost,
)
def copy_records_iterator_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    all_records = []
    for records in mdr.records_object:
        all_records.extend(records)
    to_mdr = as_records(all_records, data_format=RecordsFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[DataFrameIteratorFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[DataFrameFormat],
    cost=MemoryToMemoryCost,
)
def copy_dataframe_iterator_to_dataframe(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    all_dfs = []
    for df in mdr.records_object:
        all_dfs.append(df)
    to_mdr = as_records(pd.concat(all_dfs), data_format=DataFrameFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[DelimitedFileObjectFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsFormat],
    cost=MemoryToBufferCost,
)
def copy_file_object_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    obj = read_csv(mdr.records_object)
    to_mdr = as_records(obj, data_format=RecordsFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[DelimitedFileObjectFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsIteratorFormat],
    cost=BufferToBufferCost,
)
def copy_file_object_to_records_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    # Note: must keep header on each chunk when iterating delimited file object!
    # TODO: ugly hard-coded 1000 here, but how could we ever make it configurable? Not a big deal I guess
    itr = (
        read_csv(chunk)
        for chunk in with_header(iterate_chunks(mdr.records_object, 1000))
    )
    to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[DelimitedFileObjectIteratorFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsIteratorFormat],
    cost=BufferToBufferCost,
)
def copy_file_object_iterator_to_records_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    itr = (read_csv(chunk) for chunk in with_header(mdr.records_object))
    to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)
