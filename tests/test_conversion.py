from __future__ import annotations

import tempfile
from typing import Optional

import pytest

from basis.core.conversion import (
    StorageFormat,
    convert_lowest_cost,
    convert_sdb,
    get_converter_lookup,
)
from basis.core.conversion.converter import Conversion, ConversionCostLevel
from basis.core.data_block import DataBlockMetadata, create_data_block_from_records
from basis.core.data_formats import (
    DataFormat,
    RecordsListFormat,
    DatabaseTableFormat,
    DataFrameFormat,
    RecordsListGeneratorFormat,
    DatabaseTableRefFormat,
    JsonListFileFormat,
    DelimitedFileFormat,
    DatabaseCursorFormat,
)
from basis.core.storage.storage import StorageType, new_local_memory_storage
from tests.utils import (
    TestType1,
    TestType4,
    df_generic,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_env,
    make_test_execution_context,
)


@pytest.mark.parametrize(
    "conversion,expected_cost",
    [
        # Memory to DB
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value,
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value
            + ConversionCostLevel.MEMORY.value,  # To RecordsList, then to DB
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, RecordsListGeneratorFormat),
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value,
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DatabaseTableRefFormat),
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value,  # TODO Not really over the wire! Converter doesn't understand the REF is free
        ),
        # DB to memory
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
                StorageFormat(StorageType.DICT_MEMORY, DatabaseTableRefFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value,  # TODO see above
        ),
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value,
        ),
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
                StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value
            + ConversionCostLevel.MEMORY.value,  # DB -> Records -> DF
        ),
        # Memory to memory
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
                StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),
            ),
            ConversionCostLevel.MEMORY.value,
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
            ),
            ConversionCostLevel.MEMORY.value,
        ),
        # Memory to DB round-trip
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DatabaseTableRefFormat),
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value
            * 2,  # TODO Not really 2x! Converter doesn't understand the REF is free
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
                StorageFormat(StorageType.DICT_MEMORY, DatabaseTableRefFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value * 2,
            # TODO see above
        ),
        # DB to memory round-trip
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DatabaseTableFormat),
                StorageFormat(StorageType.MYSQL_DATABASE, DatabaseTableFormat),
            ),
            ConversionCostLevel.OVER_WIRE.value * 2,
        ),
        # Unsupported conversions (currently)
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DatabaseCursorFormat),
                StorageFormat(StorageType.MYSQL_DATABASE, DatabaseTableFormat),
            ),
            None,
        ),
        # File system
        (
            (
                StorageFormat(StorageType.LOCAL_FILE_SYSTEM, DelimitedFileFormat),
                StorageFormat(StorageType.DICT_MEMORY, RecordsListGeneratorFormat),
            ),
            ConversionCostLevel.DISK.value,
        ),
        (
            (
                StorageFormat(StorageType.LOCAL_FILE_SYSTEM, JsonListFileFormat),
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
            ),
            ConversionCostLevel.DISK.value,
        ),
    ],
)
def test_conversion_costs(conversion: Conversion, expected_cost: Optional[int]):
    cp = get_converter_lookup().get_lowest_cost_path(conversion)
    if expected_cost is None:
        assert cp is None
    else:
        assert cp.total_cost == expected_cost


class TestConversions:
    def setup(self):
        env = make_test_env()
        self.env = env
        dir = tempfile.gettempdir()
        self.fs = self.env.add_storage(f"file://{dir}")
        # self.pg = self.env.add_storage("sqlite://")
        self.db = DataBlockMetadata(
            expected_otype_uri="_test.TestType4", realized_otype_uri="_test.TestType4"
        )
        self.records = [
            {"f1": "hi", "f2": 2},
            {"f1": "bye", "f2": 3},
            {"f1": "bye"},
        ]
        self.records_full = [
            {"f1": "hi", "f2": 2},
            {"f1": "bye", "f2": 3},
            {"f1": "bye", "f2": None},
        ]
        self.local_storage = new_local_memory_storage()
        self.sess = self.env.get_new_metadata_session()
        self.db, self.sdb = create_data_block_from_records(
            self.env,
            self.sess,
            self.local_storage,
            self.records,
            expected_otype=TestType4,
        )
        assert self.sdb.data_format == RecordsListFormat

    def test_memory_to_file(self):
        ec = self.env.get_execution_context(self.sess)
        for fmt in (DelimitedFileFormat, JsonListFileFormat):
            out_sdb = convert_lowest_cost(ec, self.sdb, self.fs, fmt)
            assert out_sdb.data_format == fmt
            assert out_sdb.get_expected_otype(self.env) is TestType4
            assert out_sdb.get_realized_otype(self.env) is TestType4
            fsapi = self.fs.get_file_system_api(self.env)
            assert fsapi.exists(out_sdb)
            db = out_sdb.data_block.as_managed_data_block(ec)
            assert db.as_records_list() == self.records_full

    # def test_memory_to_database(self):
    #     ec = self.env.get_execution_context(self.sess)
    #     out_sdb = convert_lowest_cost(ec, self.sdb, self.fs, DatabaseTableFormat)
    #     assert out_sdb.data_format == DelimitedFileFormat
    #     assert out_sdb.get_expected_otype(self.env) is TestType4
    #     assert out_sdb.get_realized_otype(self.env) is TestType4
    #     fsapi = self.fs.get_file_system_api(self.env)
    #     assert fsapi.exists(out_sdb)
    #     db = out_sdb.data_block.as_managed_data_block(ec)
    #     assert db.as_records_list() == self.records_full
