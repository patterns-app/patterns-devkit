from collections.abc import Iterator
from io import IOBase
from typing import Sequence

from snapflow.schema.base import Schema
from snapflow.storage.data_copy.base import Conversion, DiskToMemoryCost, datacopy
from snapflow.storage.data_formats import (
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFormat,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.data_formats.delimited_file import DelimitedFileFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
)
from snapflow.storage.db.api import DatabaseStorageApi
from snapflow.storage.file_system import FileSystemStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    PythonStorageApi,
    PythonStorageClass,
    StorageApi,
)
from snapflow.utils.data import SampleableIO, write_csv


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsFormat, RecordsIteratorFormat],
    to_storage_classes=[FileSystemStorageClass],
    to_data_formats=[DelimitedFileFormat],
    cost=DiskToMemoryCost,
)
def copy_records_to_delim_file(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, FileSystemStorageApi)
    mdr = from_storage_api.get(from_name)
    records_iterator = mdr.records_object
    if not isinstance(mdr.records_object, Iterator):
        records_iterator = [records_iterator]
    with to_storage_api.open(to_name, "w") as f:
        append = False
        for records in records_iterator:
            write_csv(records, f, append=append)
            append = True


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[DelimitedFileObjectFormat, DelimitedFileObjectIteratorFormat],
    to_storage_classes=[FileSystemStorageClass],
    to_data_formats=[DelimitedFileFormat],
    cost=DiskToMemoryCost,
)
def copy_file_object_to_delim_file(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, FileSystemStorageApi)
    mdr = from_storage_api.get(from_name)
    file_obj_iterator = mdr.records_object
    if isinstance(mdr.records_object, IOBase):
        file_obj_iterator = [file_obj_iterator]
    with to_storage_api.open(to_name, "w") as to_file:
        for file_obj in file_obj_iterator:
            to_file.write(file_obj)
