from typing import Sequence

from basis.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
    logger,
)
from basis.core.data_block import StoredDataBlockMetadata
from basis.core.data_format import DataFormat
from basis.core.storage.storage import LocalMemoryStorageEngine, StorageType


class MemoryToFileConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.RECORDS_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.RECORDS_LIST_GENERATOR),
        # StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),  # Note: supporting this via MemoryToMemory
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.LOCAL_FILE_SYSTEM, DataFormat.JSON_LIST_FILE),
        StorageFormat(StorageType.LOCAL_FILE_SYSTEM, DataFormat.DELIMITED_FILE),
    )
    cost_level = ConversionCostLevel.DISK

    def _convert(
        self, input_sdb: StoredDataBlockMetadata, output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_memory_storage = LocalMemoryStorageEngine(self.env, input_sdb.storage)
        input_ldr = input_memory_storage.get_local_memory_data_records(input_sdb)
        records_objects = input_ldr.records_object
        if input_sdb.data_format == DataFormat.RECORDS_LIST:
            records_objects = [records_objects]
        if input_sdb.data_format == DataFormat.RECORDS_LIST_GENERATOR:
            records_objects = records_objects.get_generator()
        output_api = output_sdb.storage.get_file_system_api(self.env)
        if output_sdb.data_format == DataFormat.DELIMITED_FILE:
            output_api.write_records_to_csv(
                output_sdb, records_iterable=records_objects
            )
        elif output_sdb.data_format == DataFormat.JSON_LIST_FILE:
            output_api.write_records_as_json(
                output_sdb, records_iterable=records_objects
            )
        else:
            raise NotImplementedError(output_sdb.data_format)
        return output_sdb
