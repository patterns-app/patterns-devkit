from __future__ import annotations

import tempfile
import types
from io import StringIO
from itertools import product
from typing import Any, List, Optional, Tuple, Type

import pandas as pd
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
from snapflow.storage.data_formats.base import DataFormat
from snapflow.storage.data_formats.data_frame import DataFrameIteratorFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
)
from snapflow.storage.data_records import MemoryDataRecords, as_records
from snapflow.storage.db.api import DatabaseApi, DatabaseStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    LocalPythonStorageEngine,
    PostgresStorageEngine,
    PythonStorageApi,
    PythonStorageClass,
    Storage,
    clear_local_storage,
    new_local_python_storage,
)
from snapflow.utils.pandas import assert_dataframes_are_almost_equal
from tests.utils import TestSchema1, TestSchema4

records = [{"f1": "hi", "f2": 1}, {"f1": "bye", "f2": 2}]
rf = (RecordsFormat, lambda: records)
dff = (DataFrameFormat, lambda: pd.DataFrame.from_records(records))
dlff = (DelimitedFileObjectFormat, lambda: StringIO("f1,f2\nhi,1\nbye,2"))
rif = (RecordsIteratorFormat, lambda: ([r] for r in records))
dfif = (DataFrameIteratorFormat, lambda: (pd.DataFrame([r]) for r in records))
from_formats = [rf, dff, dfif, rif, dlff]
to_formats = [rf, dff]


@pytest.mark.parametrize(
    "from_fmt,to_fmt",
    product(from_formats, to_formats),
)
def test_mem_to_mem(from_fmt, to_fmt):
    from_fmt, obj = from_fmt
    to_fmt, expected = to_fmt
    if from_fmt == to_fmt:
        return
    mem_api: PythonStorageApi = new_local_python_storage().get_api()
    from_name = "_from_test"
    to_name = "_to_test"
    mem_api.put(from_name, as_records(obj(), data_format=from_fmt))
    conversion = Conversion(
        StorageFormat(LocalPythonStorageEngine, from_fmt),
        StorageFormat(LocalPythonStorageEngine, to_fmt),
    )
    pth = get_datacopy_lookup().get_lowest_cost_path(conversion)
    for i, ce in enumerate(pth.conversions):
        ce.copier.copy(
            from_name, to_name, ce.conversion, mem_api, mem_api, schema=TestSchema4
        )
        from_name = to_name
        to_name = to_name + str(i)
    to_name = from_name
    if isinstance(expected, pd.DataFrame):
        assert_dataframes_are_almost_equal(
            mem_api.get(to_name).records_object, expected
        )
    else:
        assert list(mem_api.get(to_name).records_object) == list(expected())
