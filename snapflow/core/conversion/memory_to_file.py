import shutil
from io import IOBase
from typing import Iterator, Sequence

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
    RecordsListIteratorFormat,
)
from snapflow.core.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
)
from snapflow.core.storage.storage import LocalMemoryStorageEngine, StorageType


class MemoryToFileConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
        StorageFormat(StorageType.DICT_MEMORY, RecordsListIteratorFormat),
        StorageFormat(StorageType.DICT_MEMORY, DelimitedFileObjectFormat),
        StorageFormat(StorageType.DICT_MEMORY, DelimitedFileObjectIteratorFormat),
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
        output_api = output_sdb.storage.get_file_system_api(self.env)
        if input_sdb.data_format == DelimitedFileObjectFormat:
            if output_sdb.data_format == DelimitedFileFormat:
                self.file_object_to_file(records_objects, output_sdb)
            else:
                raise NotImplementedError
        elif input_sdb.data_format == DelimitedFileObjectIteratorFormat:
            if output_sdb.data_format == DelimitedFileFormat:
                self.file_object_iterator_to_file(records_objects, output_sdb)
            else:
                raise NotImplementedError
        else:
            if input_sdb.data_format == RecordsListFormat:
                records_objects = [records_objects]
            if input_sdb.data_format == RecordsListIteratorFormat:
                records_objects = records_objects
            if output_sdb.data_format == DelimitedFileFormat:
                output_api.write_records_to_csv(
                    self.sess, output_sdb, records_iterable=records_objects
                )
            elif output_sdb.data_format == JsonListFileFormat:
                output_api.write_records_as_json(
                    output_sdb, records_iterable=records_objects
                )
            else:
                raise NotImplementedError(output_sdb.data_format)
        return output_sdb

    def file_object_to_file(
        self,
        file_obj: IOBase,
        output_sdb: StoredDataBlockMetadata,
    ):
        output_api = output_sdb.storage.get_file_system_api(self.env)
        path = output_api.get_path(output_sdb)
        with open(path, "w") as dst:
            file_obj.seek(0)
            shutil.copyfileobj(file_obj, dst)

    def file_object_iterator_to_file(
        self,
        file_objs: Iterator[IOBase],
        output_sdb: StoredDataBlockMetadata,
    ):
        output_api = output_sdb.storage.get_file_system_api(self.env)
        path = output_api.get_path(output_sdb)
        with open(path, "w") as dst:
            for file_obj in file_objs:
                file_obj.seek(0)
                shutil.copyfileobj(file_obj, dst)
