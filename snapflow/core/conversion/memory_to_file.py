from typing import Sequence

from snapflow.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
)
from snapflow.core.data_block import StoredDataBlockMetadata
from snapflow.core.data_formats import (
    DataFormat,
    DelimitedFileFormat,
    JsonListFileFormat,
    RecordsListFormat,
    RecordsListGeneratorFormat,
)
from snapflow.core.storage.storage import LocalMemoryStorageEngine, StorageType


class MemoryToFileConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
        StorageFormat(StorageType.DICT_MEMORY, RecordsListGeneratorFormat),
        # StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),  # Note: supporting this via MemoryToMemory
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.LOCAL_FILE_SYSTEM, JsonListFileFormat),
        StorageFormat(StorageType.LOCAL_FILE_SYSTEM, DelimitedFileFormat),
    )
    cost_level = ConversionCostLevel.DISK

    def _convert(
        self,
        input_sdb: StoredDataBlockMetadata,
        output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_memory_storage = LocalMemoryStorageEngine(self.env, input_sdb.storage)
        input_ldr = input_memory_storage.get_local_memory_data_records(input_sdb)
        records_objects = input_ldr.records_object
        if input_sdb.data_format == RecordsListFormat:
            records_objects = [records_objects]
        if input_sdb.data_format == RecordsListGeneratorFormat:
            records_objects = records_objects.get_generator()
        output_api = output_sdb.storage.get_file_system_api(self.env)
        if output_sdb.data_format == DelimitedFileFormat:
            output_api.write_records_to_csv(
                output_sdb, records_iterable=records_objects
            )
        elif output_sdb.data_format == JsonListFileFormat:
            output_api.write_records_as_json(
                output_sdb, records_iterable=records_objects
            )
        else:
            raise NotImplementedError(output_sdb.data_format)
        return output_sdb
