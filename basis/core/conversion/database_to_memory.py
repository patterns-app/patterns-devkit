from typing import Any, Sequence

import pandas as pd

from basis.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
)
from basis.core.data_block import LocalMemoryDataRecords, StoredDataBlockMetadata
from basis.core.data_format import DatabaseTable, DataFormat, RecordsListGenerator
from basis.core.storage import LocalMemoryStorageEngine, StorageType
from basis.db.utils import db_result_batcher, result_proxy_to_records_list


class DatabaseToMemoryConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.MYSQL_DATABASE, DataFormat.DATABASE_TABLE),
        StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
        StorageFormat(StorageType.SQLITE_DATABASE, DataFormat.DATABASE_TABLE),
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.RECORDS_LIST),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_TABLE_REF),
        StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_CURSOR),
    )
    cost_level = ConversionCostLevel.OVER_WIRE

    def _convert(
        self, input_sdb: StoredDataBlockMetadata, output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_runtime = input_sdb.storage.get_database_api(self.env)
        output_memory_storage = LocalMemoryStorageEngine(self.env, output_sdb.storage)
        name = input_sdb.get_name(self.env)
        db_conn = input_runtime.get_connection()
        output_records: Any
        select_sql = f"select * from {name}"
        if output_sdb.data_format == DataFormat.DATABASE_TABLE_REF:
            output_records = DatabaseTable(name, storage_url=input_sdb.storage_url)
        elif output_sdb.data_format == DataFormat.DATABASE_CURSOR:
            output_records = db_conn.execute(select_sql)
        # TODO: We are not using Pandas type inference generally, so can't use read_sql or any other read_* method
        #   so this goes through DB -> RECORDS_LIST -> DATAFRAME conversion for now
        # elif output_sdb.data_format == DataFormat.DATAFRAME:
        #     output_records = pd.read_sql_table(name, con=db_conn)
        elif output_sdb.data_format == DataFormat.RECORDS_LIST:
            output_records = result_proxy_to_records_list(db_conn.execute(select_sql))
        elif output_sdb.data_format == DataFormat.RECORDS_LIST_GENERATOR:
            output_records = RecordsListGenerator(
                db_result_batcher(db_conn.execute(select_sql))
            )
        else:
            raise NotImplementedError(output_sdb.data_format)
        ldr = LocalMemoryDataRecords.from_records_object(
            output_records, data_format=output_sdb.data_format
        )
        output_memory_storage.store_local_memory_data_records(output_sdb, ldr)
        return output_sdb
