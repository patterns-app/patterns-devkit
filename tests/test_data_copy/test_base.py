from __future__ import annotations

import tempfile
import types
from io import StringIO
from typing import Optional, Type

import pytest
from snapflow.core.data_block import DataBlockMetadata, create_data_block_from_records
from snapflow.storage.data_copy.base import (
    Conversion,
    DataCopier,
    NetworkToMemoryCost,
    NoOpCost,
    StorageFormat,
    datacopy,
    get_datacopy_lookup,
)
from snapflow.storage.data_copy.database_to_memory import copy_db_to_records
from snapflow.storage.data_copy.memory_to_database import copy_records_to_db
from snapflow.storage.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFrameFormat,
    DelimitedFileFormat,
    JsonLinesFileFormat,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.data_formats.data_frame import DataFrameIteratorFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
)
from snapflow.storage.data_records import MemoryDataRecords, as_records
from snapflow.storage.db.api import DatabaseApi, DatabaseStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    LocalFileSystemStorageEngine,
    LocalPythonStorageEngine,
    PostgresStorageEngine,
    PythonStorageApi,
    PythonStorageClass,
    SqliteStorageEngine,
    Storage,
    clear_local_storage,
    new_local_python_storage,
)
from tests.utils import TestSchema1, TestSchema4


def test_data_copy_decorator():
    @datacopy(cost=NoOpCost, unregistered=True)
    def copy(*args):
        pass

    assert copy.cost is NoOpCost


def test_data_copy_lookup():
    @datacopy(
        cost=NoOpCost, from_storage_classes=[FileSystemStorageClass], unregistered=True
    )
    def noop_all(*args):
        pass

    @datacopy(
        from_storage_classes=[DatabaseStorageClass],
        from_data_formats=[DatabaseTableFormat],
        to_storage_classes=[PythonStorageClass],
        to_data_formats=[RecordsFormat],
        cost=NetworkToMemoryCost,
        unregistered=True,
    )
    def db_to_mem(*args):
        pass

    lkup = get_datacopy_lookup(copiers=[noop_all, db_to_mem])
    dcp = lkup.get_lowest_cost(
        Conversion(
            StorageFormat(PostgresStorageEngine, DatabaseTableFormat),
            StorageFormat(LocalPythonStorageEngine, RecordsFormat),
        )
    )
    assert dcp is db_to_mem


@pytest.mark.parametrize(
    "conversion,length",
    [
        # Memory to DB
        (
            (
                StorageFormat(LocalPythonStorageEngine, RecordsFormat),
                StorageFormat(PostgresStorageEngine, DatabaseTableFormat),
            ),
            1,
        ),
        (
            (
                StorageFormat(LocalPythonStorageEngine, DataFrameFormat),
                StorageFormat(PostgresStorageEngine, DatabaseTableFormat),
            ),
            2,
        ),
        (
            (
                StorageFormat(LocalPythonStorageEngine, DataFrameIteratorFormat),
                StorageFormat(LocalPythonStorageEngine, DataFrameFormat),
            ),
            1,
        ),
        (
            (
                StorageFormat(LocalPythonStorageEngine, DataFrameIteratorFormat),
                StorageFormat(LocalPythonStorageEngine, RecordsIteratorFormat),
            ),
            1,
        ),
        (
            (
                StorageFormat(LocalPythonStorageEngine, RecordsIteratorFormat),
                StorageFormat(PostgresStorageEngine, DatabaseTableFormat),
            ),
            1,
        ),
        (
            (
                StorageFormat(LocalPythonStorageEngine, RecordsIteratorFormat),
                StorageFormat(SqliteStorageEngine, DatabaseTableFormat),
            ),
            1,
        ),
        (
            (
                StorageFormat(LocalPythonStorageEngine, DelimitedFileObjectFormat),
                StorageFormat(SqliteStorageEngine, DatabaseTableFormat),
            ),
            2,
        ),
        (
            (
                StorageFormat(LocalFileSystemStorageEngine, DelimitedFileFormat),
                StorageFormat(SqliteStorageEngine, DatabaseTableFormat),
            ),
            3,  # file -> file obj -> records iter -> db table
        ),
    ],
)
def test_conversion_costs(conversion: Conversion, length: Optional[int]):
    cp = get_datacopy_lookup().get_lowest_cost_path(Conversion(*conversion))
    if length is None:
        assert cp is None
    else:
        assert cp is not None
        # for c in cp.conversions:
        #     print(f"{c.copier.copier_function} {c.conversion}")
        assert len(cp.conversions) == length
