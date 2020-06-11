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
    DictList,
    DictListGenerator,
)
from basis.core.storage import LocalMemoryStorageEngine, StorageType


class MemoryToMemoryConverter(Converter):
    # TODO: we DON'T in general want to convert a GENERATOR to a non-GENERATOR
    #   currently this might happen if we get a DataFrameGenerator, conversion path
    #   might be: DFG -> DictList -> DBTable. What we want is DFG -> DLG -> DBTable
    #   Solution is to add differing costs i suppose
    # TODO: parameterized costs
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST_GENERATOR),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME_GENERATOR),
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
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
            (DataFormat.DATAFRAME, DataFormat.DICT_LIST): self.dataframe_to_dictlist,
            (DataFormat.DICT_LIST, DataFormat.DATAFRAME): self.dictlist_to_dataframe,
            (
                DataFormat.DICT_LIST_GENERATOR,
                DataFormat.DATAFRAME,
            ): self.dictlist_generator_to_dataframe,
            (
                DataFormat.DICT_LIST_GENERATOR,
                DataFormat.DICT_LIST,
            ): self.dictlist_generator_to_dictlist,
            (
                DataFormat.DATAFRAME_GENERATOR,
                DataFormat.DATAFRAME,
            ): self.dataframe_generator_to_dataframe,
            (
                DataFormat.DATAFRAME_GENERATOR,
                DataFormat.DICT_LIST,
            ): self.dataframe_generator_to_dictlist,
        }
        try:
            output_records_object = lookup[
                (input_sdb.data_format, output_sdb.data_format)
            ](input_ldr.records_object)
        except KeyError:
            raise NotImplementedError((input_sdb.data_format, output_sdb.data_format))
        output_ldr = LocalMemoryDataRecords.from_records_object(output_records_object)
        output_memory_storage.store_local_memory_data_records(output_sdb, output_ldr)
        return output_sdb

    def dataframe_to_dictlist(self, input_object: DataFrame) -> DictList:
        return input_object.to_dict(orient="records")

    def dictlist_to_dataframe(self, input_object: DictList) -> DataFrame:
        return DataFrame(input_object)

    def dictlist_generator_to_dictlist(
        self, input_object: DictListGenerator
    ) -> DictList:
        all_ = []
        for dl in input_object.get_generator():
            all_.extend(dl)
        return all_

    def dictlist_generator_to_dataframe(
        self, input_object: DictListGenerator
    ) -> DataFrame:
        return self.dictlist_to_dataframe(
            self.dictlist_generator_to_dictlist(input_object)
        )

    def dataframe_generator_to_dictlist(
        self, input_object: DataFrameGenerator
    ) -> DictList:
        return self.dataframe_to_dictlist(
            self.dataframe_generator_to_dataframe(input_object)
        )

    def dataframe_generator_to_dataframe(
        self, input_object: DataFrameGenerator
    ) -> DataFrame:
        all_ = []
        for dl in input_object.get_generator():
            all_.append(dl)
        return concat(all_)
