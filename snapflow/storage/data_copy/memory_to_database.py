from typing import Sequence

from snapflow.schema.base import Schema
from snapflow.storage.data_copy.base import (
    Conversion,
    DiskToMemoryCost,
    NetworkToBufferCost,
    NetworkToMemoryCost,
    datacopy,
)
from snapflow.storage.data_formats import (
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFormat,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.db.api import DatabaseStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    PythonStorageApi,
    PythonStorageClass,
    StorageApi,
)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsFormat],
    to_storage_classes=[DatabaseStorageClass],
    to_data_formats=[DatabaseTableFormat],
    cost=NetworkToMemoryCost,
)
def copy_records_to_db(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, DatabaseStorageApi)
    mdr = from_storage_api.get(from_name)
    to_storage_api.bulk_insert_records(to_name, mdr.records_object, schema)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsIteratorFormat],
    to_storage_classes=[DatabaseStorageClass],
    to_data_formats=[DatabaseTableFormat],
    cost=NetworkToBufferCost,
)
def copy_records_iterator_to_db(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, DatabaseStorageApi)
    mdr = from_storage_api.get(from_name)
    for records in mdr.records_object:
        to_storage_api.bulk_insert_records(to_name, records, schema)
