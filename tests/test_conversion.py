from __future__ import annotations

import tempfile
from typing import Optional

import pytest

from dags.core.conversion import (
    StorageFormat,
    convert_lowest_cost,
    get_converter_lookup,
)
from dags.core.conversion.converter import Conversion, ConversionCostLevel
from dags.core.data_block import DataBlockMetadata, create_data_block_from_records
from dags.core.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFrameFormat,
    DelimitedFileFormat,
    JsonListFileFormat,
    RecordsListFormat,
    RecordsListGeneratorFormat,
)
from dags.core.graph import Graph
from dags.core.storage.storage import StorageType, new_local_memory_storage
from tests.test_utils import get_tmp_sqlite_db_url
from tests.utils import TestSchema4, make_test_env


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
            expected_schema_key="_test.TestSchema4",
            realized_schema_key="_test.TestSchema4",
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
            expected_schema=TestSchema4,
        )
        assert self.sdb.data_format == RecordsListFormat

    def test_memory_to_file(self):
        g = Graph(self.env)
        ec = self.env.get_execution_context(g, self.sess)
        for fmt in (DelimitedFileFormat, JsonListFileFormat):
            out_sdb = convert_lowest_cost(ec, self.sdb, self.fs, fmt)
            assert out_sdb.data_format == fmt
            assert out_sdb.get_expected_schema(self.env) is TestSchema4
            assert out_sdb.get_realized_schema(self.env) is TestSchema4
            fsapi = self.fs.get_file_system_api(self.env)
            assert fsapi.exists(out_sdb)
            db = out_sdb.data_block.as_managed_data_block(ec)
            assert db.as_records_list() == self.records_full

    def test_memory_to_database(self):
        g = Graph(self.env)
        database = self.env.add_storage(get_tmp_sqlite_db_url())
        ec = self.env.get_execution_context(g, self.sess)
        out_sdb = convert_lowest_cost(ec, self.sdb, database, DatabaseTableFormat)
        assert out_sdb.data_format == DatabaseTableFormat
        assert out_sdb.get_expected_schema(self.env) is TestSchema4
        assert out_sdb.get_realized_schema(self.env) is TestSchema4
        assert database.get_database_api(self.env).exists(out_sdb.get_name(self.env))
        db = out_sdb.data_block.as_managed_data_block(ec)
        assert db.as_records_list() == self.records_full
