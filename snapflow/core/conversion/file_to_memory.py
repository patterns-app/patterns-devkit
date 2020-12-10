from io import IOBase
from typing import Any, Generator, Sequence

import pandas as pd
from snapflow.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
)
from snapflow.core.data_block import LocalMemoryDataRecords, StoredDataBlockMetadata
from snapflow.core.data_formats import (
    DataFormat,
    DelimitedFileFormat,
    JsonListFileFormat,
    RecordsListFormat,
    RecordsListGenerator,
    RecordsListGeneratorFormat,
)
from snapflow.core.storage.file_system import FileSystemAPI
from snapflow.core.storage.storage import LocalMemoryStorageEngine, StorageType
from snapflow.core.typing.inference import conform_records_list_to_schema
from snapflow.utils.data import read_csv, read_json


class FileToMemoryConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.LOCAL_FILE_SYSTEM, DelimitedFileFormat),
        StorageFormat(StorageType.LOCAL_FILE_SYSTEM, JsonListFileFormat),
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
        StorageFormat(StorageType.DICT_MEMORY, RecordsListGeneratorFormat),
    )
    cost_level = ConversionCostLevel.DISK

    def _convert(
        self,
        input_sdb: StoredDataBlockMetadata,
        output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_api = input_sdb.storage.get_file_system_api(self.env)
        output_memory_storage = LocalMemoryStorageEngine(self.env, output_sdb.storage)
        output_records: Any
        if output_sdb.data_format == RecordsListFormat:
            if input_sdb.data_format == DelimitedFileFormat:
                with input_api.open(input_sdb) as f:
                    output_records = conform_records_list_to_schema(
                        read_csv(f.readlines()), input_sdb.realized_schema(self.env)
                    )
            elif input_sdb.data_format == JsonListFileFormat:
                with input_api.open(input_sdb) as f:
                    output_records = [
                        read_json(line) for line in f.readlines()
                    ]  # TODO: ensure string not bytes
            else:
                raise NotImplementedError(input_sdb.data_format)
        elif output_sdb.data_format == RecordsListGeneratorFormat:
            if input_sdb.data_format == DelimitedFileFormat:
                output_records = self.delimited_file_to_records_list_generator(
                    input_api, input_sdb
                )
            elif input_sdb.data_format == JsonListFileFormat:
                output_records = self.json_list_file_to_records_list_generator(
                    input_api, input_sdb
                )
            else:
                raise NotImplementedError(input_sdb.data_format)
        else:
            raise NotImplementedError(output_sdb.data_format)
        ldr = LocalMemoryDataRecords.from_records_object(
            output_records, data_format=output_sdb.data_format
        )
        output_memory_storage.store_local_memory_data_records(output_sdb, ldr)
        return output_sdb

    def delimited_file_to_records_list_generator(
        self, input_api: FileSystemAPI, input_sdb: StoredDataBlockMetadata
    ) -> RecordsListGenerator:
        def generate_chunks(
            sdb: StoredDataBlockMetadata, chunk_size: int = 1000
        ) -> Generator:
            with input_api.open(sdb) as f:
                header = f.readline()
                line = header
                while line:
                    chunk = [header]
                    line = f.readline()
                    while len(chunk) < chunk_size and line:
                        chunk.append(line)
                        line = f.readline()
                    yield conform_records_list_to_schema(
                        read_csv(chunk), input_sdb.realized_schema(self.env)
                    )

        return RecordsListGenerator(generate_chunks(input_sdb))

    def json_list_file_to_records_list_generator(
        self, input_api: FileSystemAPI, input_sdb: StoredDataBlockMetadata
    ) -> RecordsListGenerator:
        def generate_chunks(
            sdb: StoredDataBlockMetadata, chunk_size: int = 1000
        ) -> Generator:
            with input_api.open(sdb) as f:
                chunk = []
                for line in f.readlines():
                    chunk.append(line)
                    if len(chunk) >= chunk_size:
                        yield [read_json(line) for line in chunk]
                        chunk = []

        return RecordsListGenerator(generate_chunks(input_sdb))
