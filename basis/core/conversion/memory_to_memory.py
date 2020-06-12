from typing import Sequence

from pandas import DataFrame, concat

from basis.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
)
from basis.core.data_block import LocalMemoryDataRecords, StoredDataBlockMetadata
from basis.core.data_format import (
    DataFormat,
    DataFrameGenerator,
    RecordsList,
    RecordsListGenerator,
)
from basis.core.storage import LocalMemoryStorageEngine, StorageType
from basis.core.typing.object_type import ObjectType
from basis.utils.pandas import dataframe_to_records_list, records_list_to_dataframe


class MemoryToMemoryConverter(Converter):
    # TODO: we DON'T in general want to convert a GENERATOR to a non-GENERATOR
    #   currently this might happen if we get a DataFrameGenerator, conversion path
    #   might be: DFG -> RecordsList -> DBTable. What we want is DFG -> DLG -> DBTable
    #   Solution is to add differing costs i suppose
    # TODO: parameterized costs
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.RECORDS_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.RECORDS_LIST_GENERATOR),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME_GENERATOR),
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.RECORDS_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
    )
    cost_level = ConversionCostLevel.MEMORY

    def _convert(
        self, input_sdb: StoredDataBlockMetadata, output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_memory_storage = LocalMemoryStorageEngine(self.env, input_sdb.storage)
        output_memory_storage = LocalMemoryStorageEngine(self.env, output_sdb.storage)
        input_ldr = input_memory_storage.get_local_memory_data_records(input_sdb)
        lookup = {
            (
                DataFormat.DATAFRAME,
                DataFormat.RECORDS_LIST,
            ): self.dataframe_to_records_list,
            (
                DataFormat.RECORDS_LIST,
                DataFormat.DATAFRAME,
            ): self.records_list_to_dataframe,
            (
                DataFormat.RECORDS_LIST_GENERATOR,
                DataFormat.DATAFRAME,
            ): self.records_list_generator_to_dataframe,
            (
                DataFormat.RECORDS_LIST_GENERATOR,
                DataFormat.RECORDS_LIST,
            ): self.records_list_generator_to_records_list,
            (
                DataFormat.DATAFRAME_GENERATOR,
                DataFormat.DATAFRAME,
            ): self.dataframe_generator_to_dataframe,
            (
                DataFormat.DATAFRAME_GENERATOR,
                DataFormat.RECORDS_LIST,
            ): self.dataframe_generator_to_records_list,
        }
        try:
            output_records_object = lookup[
                (input_sdb.data_format, output_sdb.data_format)
            ](
                input_ldr.records_object, input_sdb.get_realized_otype(self.env)
            )  # TODO: Realized or expected? Should be more obvious which one to use
        except KeyError:
            raise NotImplementedError((input_sdb.data_format, output_sdb.data_format))
        output_ldr = LocalMemoryDataRecords.from_records_object(output_records_object)
        output_memory_storage.store_local_memory_data_records(output_sdb, output_ldr)
        return output_sdb

    def dataframe_to_records_list(
        self, input_object: DataFrame, otype: ObjectType
    ) -> RecordsList:
        return dataframe_to_records_list(input_object, otype)

    def records_list_to_dataframe(
        self, input_object: RecordsList, otype: ObjectType
    ) -> DataFrame:
        return records_list_to_dataframe(input_object, otype)

    def records_list_generator_to_records_list(
        self, input_object: RecordsListGenerator, otype: ObjectType
    ) -> RecordsList:
        all_ = []
        for dl in input_object.get_generator():
            all_.extend(dl)
        return all_

    def records_list_generator_to_dataframe(
        self, input_object: RecordsListGenerator, otype: ObjectType
    ) -> DataFrame:
        return self.records_list_to_dataframe(
            self.records_list_generator_to_records_list(input_object, otype), otype
        )

    def dataframe_generator_to_records_list(
        self, input_object: DataFrameGenerator, otype: ObjectType
    ) -> RecordsList:
        return self.dataframe_to_records_list(
            self.dataframe_generator_to_dataframe(input_object, otype), otype
        )

    def dataframe_generator_to_dataframe(
        self, input_object: DataFrameGenerator, otype: ObjectType
    ) -> DataFrame:
        all_ = []
        for dl in input_object.get_generator():
            all_.append(dl)
        return concat(all_)
