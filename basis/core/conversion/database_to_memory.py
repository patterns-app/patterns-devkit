from typing import Sequence

import pandas as pd

from basis.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
)
from basis.core.data_format import DatabaseTable, DataFormat
from basis.core.data_resource import LocalMemoryDataRecords, StoredDataResourceMetadata
from basis.core.storage_resource import LocalMemoryStorage, StorageType


class DatabaseToMemoryConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.MYSQL_DATABASE, DataFormat.DATABASE_TABLE),
        StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_TABLE_REF),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_CURSOR),
    )
    cost_level = ConversionCostLevel.OVER_WIRE

    def _convert(
        self,
        input_sdr: StoredDataResourceMetadata,
        output_sdr: StoredDataResourceMetadata,
    ) -> StoredDataResourceMetadata:
        input_runtime = input_sdr.storage_resource.get_database_api(self.env)
        output_memory_storage = LocalMemoryStorage(
            self.env, output_sdr.storage_resource
        )
        name = input_sdr.get_name(self.env)
        db_conn = input_runtime.get_connection()
        if output_sdr.data_format == DataFormat.DATABASE_TABLE_REF:
            output_records = DatabaseTable(
                name, storage_resource_url=input_sdr.storage_resource_url
            )
        elif output_sdr.data_format == DataFormat.DATABASE_CURSOR:
            output_records = db_conn.execute(f"select * from {name}")
        elif output_sdr.data_format == DataFormat.DATAFRAME:
            output_records = pd.read_sql_table(name, con=db_conn)
        elif output_sdr.data_format == DataFormat.DICT_LIST:
            output_records = pd.read_sql_table(name, con=db_conn).to_dict(
                orient="records"
            )  # TODO: don't go thru pd
        else:
            raise NotImplementedError(output_sdr.data_format)
        ldr = LocalMemoryDataRecords.from_records_object(output_records)
        output_memory_storage.store_local_memory_data_records(output_sdr, ldr)
        return output_sdr
