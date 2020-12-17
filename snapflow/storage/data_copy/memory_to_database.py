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


# class MemoryToDatabaseConverter(Converter):
#     supported_input_formats: Sequence[StorageFormat] = (
#         StorageFormat(StorageType.DICT_MEMORY, RecordsFormat),
#         StorageFormat(StorageType.DICT_MEMORY, RecordsIteratorFormat),
#         # StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),  # Note: supporting this via MemoryToMemory
#         StorageFormat(StorageType.DICT_MEMORY, DatabaseTableRefFormat),
#         # StorageFormat(StorageType.DICT_MEMORY, DatabaseCursorFormat), # TODO: need to figure out how to get db url from ResultProxy
#     )
#     supported_output_formats: Sequence[StorageFormat] = (
#         StorageFormat(StorageType.MYSQL_DATABASE, DatabaseTableFormat),
#         StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
#         StorageFormat(StorageType.SQLITE_DATABASE, DatabaseTableFormat),
#     )
#     cost_level = ConversionCostLevel.OVER_WIRE

#     def _convert(
#         self,
#         input_sdb: StoredDataBlockMetadata,
#         output_sdb: StoredDataBlockMetadata,
#     ) -> StoredDataBlockMetadata:
#         input_python_storage = LocalPythonStorageEngine(self.env, input_sdb.storage)
#         input_ldr = input_python_storage.get_local_memory_data_records(input_sdb)
#         if input_sdb.data_format == DatabaseTableRefFormat:
#             if input_ldr.wrapped_records_object.storage_url == output_sdb.storage_url:
#                 # No-op, already exists (shouldn't really ever get here)
#                 logger.warning("Non-conversion to existing table requested")  # TODO
#                 return output_sdb
#             else:
#                 raise NotImplementedError(
#                     f"No inter-db migration implemented yet ({input_sdb.storage_url} to {output_sdb.storage_url})"
#                 )
#         assert input_sdb.data_format in (
#             RecordsFormat,
#             RecordsIteratorFormat,
#         )
#         records_objects = input_ldr.wrapped_records_object
#         if input_sdb.data_format == RecordsFormat:
#             records_objects = [records_objects]
#         if input_sdb.data_format == RecordsIteratorFormat:
#             records_objects = records_objects
#         output_api = output_sdb.storage.get_database_api(self.env)
#         # TODO: this loop is what is actually calling our Iterable Pipe in RECORDS_GENERATOR case. Is that ok?
#         #   Seems a bit opaque. Why don't we just iterate this at the conform_output level?
#         #   One reason would be to allow for efficiency/flexibility at the converter level
#         #   in handling chunks of data (which we don't exploit here...)
#         for records_object in records_objects:
#             output_api.bulk_insert_records(self.sess, output_sdb, records_object)
#         return output_sdb
