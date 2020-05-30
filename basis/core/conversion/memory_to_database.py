from typing import Sequence

from basis.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
    logger,
)
from basis.core.data_block import StoredDataBlockMetadata
from basis.core.data_format import DataFormat
from basis.core.storage import LocalMemoryStorageEngine, StorageType


class MemoryToDatabaseConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST_ITERATOR),
        # StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),  # Note: supporting this via MemoryToMemory
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_TABLE_REF),
        # StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_CURSOR), # TODO: need to figure out how to get db url from ResultProxy
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.MYSQL_DATABASE, DataFormat.DATABASE_TABLE),
        StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
    )
    cost_level = ConversionCostLevel.OVER_WIRE

    def _convert(
        self,
        input_sdb: StoredDataBlockMetadata,
        output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_memory_storage = LocalMemoryStorageEngine(self.env, input_sdb.storage)
        input_ldr = input_memory_storage.get_local_memory_data_records(input_sdb)
        if input_sdb.data_format == DataFormat.DATABASE_TABLE_REF:
            if input_ldr.records_object.storage_url == output_sdb.storage_url:
                # No-op, already exists (shouldn't really ever get here)
                logger.warning("Non-conversion to existing table requested")
                return output_sdb
            else:
                raise NotImplementedError(
                    f"No inter-db migration implemented yet ({input_sdb.storage_url} to {output_sdb.storage_url})"
                )
        assert input_sdb.data_format in (
            DataFormat.DICT_LIST,
            DataFormat.DICT_LIST_ITERATOR,
        )
        records_objects = input_ldr.records_object
        if input_sdb.data_format == DataFormat.DICT_LIST:
            records_objects = [records_objects]
        output_runtime = output_sdb.storage.get_database_api(self.env)
        # TODO: this loop is what is actually calling our Iterable DataFunction in DICT_LIST_ITERATOR case. Is that ok?
        #   a bit of a strange side effect of iterating a loop. what happens if there's an exception here, for instance?
        for records_object in records_objects:
            output_runtime.bulk_insert_dict_list(output_sdb, records_object)
        return output_sdb
