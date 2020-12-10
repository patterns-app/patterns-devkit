from typing import Any, Sequence

import pandas as pd
from snapflow.core.conversion.converter import (
    ConversionCostLevel,
    Converter,
    StorageFormat,
)
from snapflow.core.data_block import LocalMemoryDataRecords, StoredDataBlockMetadata
from snapflow.core.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRef,
    DatabaseTableRefFormat,
    DataFormat,
    RecordsListFormat,
    RecordsListGenerator,
    RecordsListGeneratorFormat,
)
from snapflow.core.storage.storage import LocalMemoryStorageEngine, StorageType
from snapflow.db.utils import db_result_batcher, result_proxy_to_records_list


class DatabaseToMemoryConverter(Converter):
    supported_input_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.MYSQL_DATABASE, DatabaseTableFormat),
        StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
        StorageFormat(StorageType.SQLITE_DATABASE, DatabaseTableFormat),
    )
    supported_output_formats: Sequence[StorageFormat] = (
        StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
        # StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),
        StorageFormat(StorageType.DICT_MEMORY, DatabaseTableRefFormat),
        StorageFormat(StorageType.DICT_MEMORY, DatabaseCursorFormat),
    )
    cost_level = ConversionCostLevel.OVER_WIRE

    def _convert(
        self,
        input_sdb: StoredDataBlockMetadata,
        output_sdb: StoredDataBlockMetadata,
    ) -> StoredDataBlockMetadata:
        input_runtime = input_sdb.storage.get_database_api(self.env)
        output_memory_storage = LocalMemoryStorageEngine(self.env, output_sdb.storage)
        name = input_sdb.get_name(self.env)
        db_conn = input_runtime.get_engine()
        output_records: Any
        select_sql = f"select * from {name}"
        if output_sdb.data_format == DatabaseTableRefFormat:
            output_records = DatabaseTableRef(name, storage_url=input_sdb.storage_url)
        elif output_sdb.data_format == DatabaseCursorFormat:
            output_records = db_conn.execute(select_sql)  # TODO: close this connection?
        # Note: We are not using Pandas type inference generally (it has poor null support)
        #   so can't use read_sql or any other read_* method
        #   so we let this go through DB -> RECORDS_LIST -> DATAFRAME conversion path for now
        # elif output_sdb.data_format == DataFrameFormat:
        #     output_records = pd.read_sql_table(name, con=db_conn)
        elif output_sdb.data_format == RecordsListFormat:
            output_records = result_proxy_to_records_list(db_conn.execute(select_sql))
        elif output_sdb.data_format == RecordsListGeneratorFormat:
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
