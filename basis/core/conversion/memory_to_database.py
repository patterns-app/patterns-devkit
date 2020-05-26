from typing import Sequence

from basis.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
    logger,
)
from basis.core.data_format import DataFormat
from basis.core.data_resource import StoredDataResourceMetadata
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
        input_sdr: StoredDataResourceMetadata,
        output_sdr: StoredDataResourceMetadata,
    ) -> StoredDataResourceMetadata:
        input_memory_storage = LocalMemoryStorageEngine(self.env, input_sdr.storage)
        input_ldr = input_memory_storage.get_local_memory_data_records(input_sdr)
        if input_sdr.data_format == DataFormat.DATABASE_TABLE_REF:
            if input_ldr.records_object.storage_url == output_sdr.storage_url:
                # No-op, already exists (shouldn't really ever get here)
                logger.warning("Non-conversion to existing table requested")
                return output_sdr
            else:
                raise NotImplementedError(
                    f"No inter-db migration implemented yet ({input_sdr.storage_url} to {output_sdr.storage_url})"
                )
        assert input_sdr.data_format in (
            DataFormat.DICT_LIST,
            DataFormat.DICT_LIST_ITERATOR,
        )
        records_objects = input_ldr.records_object
        if input_sdr.data_format == DataFormat.DICT_LIST:
            records_objects = [records_objects]
        output_runtime = output_sdr.storage.get_database_api(self.env)
        # TODO: this loop is what is actually calling our Iterable DataFunction in DICT_LIST_ITERATOR case. Is that ok?
        #   a bit of a strange side effect of iterating a loop. what happens if there's an exception here, for instance?
        for records_object in records_objects:
            output_runtime.bulk_insert_dict_list(output_sdr, records_object)
        return output_sdr
