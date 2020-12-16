from __future__ import annotations

import tempfile
from io import StringIO
from typing import Optional

import pytest
from snapflow.core.conversion import (
    StorageFormat,
    convert_lowest_cost,
    get_converter_lookup,
)
from snapflow.core.conversion.converter import Conversion, ConversionCostLevel
from snapflow.core.data_block import DataBlockMetadata, create_data_block_from_records
from snapflow.core.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFrameFormat,
    DelimitedFileFormat,
    JsonListFileFormat,
    RecordsListFormat,
    RecordsListIteratorFormat,
)
from snapflow.core.data_formats.delimited_file_object import DelimitedFileObjectFormat
from snapflow.core.graph import Graph
from snapflow.core.storage.storage import StorageType, new_local_memory_storage
from snapflow.testing.utils import get_tmp_sqlite_db_url
from snapflow.utils.data import SampleableIO
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
                StorageFormat(StorageType.DICT_MEMORY, RecordsListIteratorFormat),
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
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DelimitedFileObjectFormat),
                StorageFormat(StorageType.DICT_MEMORY, RecordsListFormat),
            ),
            ConversionCostLevel.MEMORY.value,
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DelimitedFileObjectFormat),
                StorageFormat(StorageType.DICT_MEMORY, DataFrameFormat),
            ),
            ConversionCostLevel.MEMORY.value,  # TODO: should be * 2
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
                StorageFormat(StorageType.DICT_MEMORY, RecordsListIteratorFormat),
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
            nominal_schema_key="_test.TestSchema4",
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
        self.sess = self.env._get_new_metadata_session()
        self.db, self.sdb = create_data_block_from_records(
            self.env,
            self.sess,
            self.local_storage,
            self.records,
            nominal_schema=TestSchema4,
        )
        assert self.sdb.data_format == RecordsListFormat
        self.file_obj = SampleableIO(StringIO("f1,f2\nhi,2\nbye,3\nbye,"))
        self.dfo_db, self.dfo_sdb = create_data_block_from_records(
            self.env,
            self.sess,
            self.local_storage,
            self.file_obj,
            nominal_schema=TestSchema4,
        )
        assert self.sdb.data_format == RecordsListFormat

    def tearDown(self):
        self.sess.close()

    def test_memory_to_file(self):
        g = Graph(self.env)
        ec = self.env.get_run_context(g)
        for fmt in (DelimitedFileFormat, JsonListFileFormat):
            out_sdb = convert_lowest_cost(ec, self.sess, self.sdb, self.fs, fmt)
            assert out_sdb.data_format == fmt
            assert out_sdb.nominal_schema(self.env, self.sess) is TestSchema4
            assert (
                out_sdb.realized_schema(self.env, self.sess).field_names()
                == TestSchema4.field_names()
            )  # TODO: really probably want "is" here as well, but we have inferred type as BigInteger and nominal type as Integer...
            fsapi = self.fs.get_file_system_api(self.env)
            assert fsapi.exists(out_sdb)
            db = out_sdb.data_block.as_managed_data_block(ec, self.sess)
            assert db.as_records_list() == self.records_full

    def test_memory_to_database(self):
        g = Graph(self.env)
        database = self.env.add_storage(get_tmp_sqlite_db_url())
        ec = self.env.get_run_context(g)
        out_sdb = convert_lowest_cost(
            ec, self.sess, self.sdb, database, DatabaseTableFormat
        )
        assert out_sdb.data_format == DatabaseTableFormat
        assert out_sdb.nominal_schema(self.env, self.sess) is TestSchema4
        assert (
            out_sdb.realized_schema(self.env, self.sess).field_names()
            == TestSchema4.field_names()
        )
        assert database.get_database_api(self.env).exists(out_sdb.get_name(self.env))
        db = out_sdb.data_block.as_managed_data_block(ec, self.sess)
        assert db.as_records_list() == self.records_full

    def test_memory_file_obj_to_file(self):
        g = Graph(self.env)
        ec = self.env.get_run_context(g)
        for fmt in (DelimitedFileFormat,):
            out_sdb = convert_lowest_cost(ec, self.sess, self.dfo_sdb, self.fs, fmt)
            assert out_sdb.data_format == fmt
            assert out_sdb.nominal_schema(self.env, self.sess) is TestSchema4
            assert (
                out_sdb.realized_schema(self.env, self.sess).field_names()
                == TestSchema4.field_names()
            )  # TODO: really probably want "is" here as well, but we have inferred type as BigInteger and nominal type as Integer...
            fsapi = self.fs.get_file_system_api(self.env)
            assert fsapi.exists(out_sdb)
            db = out_sdb.data_block.as_managed_data_block(ec, self.sess)
            assert db.as_records_list() == self.records_full
