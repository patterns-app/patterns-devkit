from typing import Sequence

from pandas import DataFrame

from basis.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
)
from basis.core.data_block import LocalMemoryDataRecords, StoredDataBlockMetadata
from basis.core.data_format import DataFormat, DictList
from basis.core.storage import LocalMemoryStorageEngine, StorageType


class MemoryToMemoryConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
    )
    cost_level = ConversionCostLevel.MEMORY

    def _convert(
        self,
        input_sdb: StoredDataBlockMetadata,
        output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_memory_storage = LocalMemoryStorageEngine(self.env, input_sdb.storage)
        output_memory_storage = LocalMemoryStorageEngine(self.env, output_sdb.storage)
        input_ldr = input_memory_storage.get_local_memory_data_records(input_sdb)
        lookup = {
            (DataFormat.DATAFRAME, DataFormat.DICT_LIST): self.dataframe_to_dictlist,
            (DataFormat.DICT_LIST, DataFormat.DATAFRAME): self.dictlist_to_dataframe,
        }
        try:
            output_records_object = lookup[
                (input_sdb.data_format, output_sdb.data_format)
            ](input_ldr.records_object)
        except KeyError:
            raise NotImplementedError
        output_ldr = LocalMemoryDataRecords.from_records_object(output_records_object)
        output_memory_storage.store_local_memory_data_records(output_sdb, output_ldr)
        return output_sdb

    def dataframe_to_dictlist(self, input_object: DataFrame) -> DictList:
        return input_object.to_dict(orient="records")

    def dictlist_to_dataframe(self, input_object: DictList) -> DataFrame:
        return DataFrame(input_object)
