from __future__ import annotations

from dataclasses import asdict
from io import StringIO
from typing import Callable

import pandas as pd
import pyarrow as pa
import pytest
from snapflow.schema.base import SchemaTranslation
from snapflow.storage.data_formats import (
    DatabaseCursorFormat,
    DataFormatBase,
    DataFrameFormat,
    DataFrameIterator,
    DataFrameIteratorFormat,
    RecordsFormat,
    RecordsIterator,
    RecordsIteratorFormat,
)
from snapflow.storage.data_formats.arrow_table import ArrowTableFormat
from snapflow.storage.data_formats.base import DataFormat, SampleableIterator
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
)
from tests.utils import TestSchema4

# Example formats
df = pd.DataFrame({"a": range(10)})
at = pa.Table.from_pandas(df)


def delim_io():
    return StringIO("f1,f2,f3\n1,2,3")


def json_io():
    return StringIO('{"f1":1, "f2":2}\n{"f1":1, "f2":2}')  # TODO


def test_definitely_is_instance():
    definitely_instances = [
        (at, [ArrowTableFormat]),
        (df, [DataFrameFormat]),
        (SampleableIterator(d for d in [df]), [DataFrameIteratorFormat]),
        ([{}, {}], [RecordsFormat]),
        (SampleableIterator(r for r in [[{}], [{}]]), [RecordsIteratorFormat]),
        (delim_io(), [DelimitedFileObjectFormat]),
        (
            SampleableIterator(r for r in [delim_io()]),
            [DelimitedFileObjectIteratorFormat],
        ),
    ]
    for obj, formats in definitely_instances:
        for fmt in formats:
            assert fmt.definitely_instance(obj)


def test_maybe_is_instance():
    maybe_instances = [
        (at, [ArrowTableFormat]),
        (df, [DataFrameFormat]),
        (
            (d for d in range(10)),
            [
                DataFrameIteratorFormat,
                RecordsIteratorFormat,
                DelimitedFileObjectIteratorFormat,
            ],
        ),
        ([], [RecordsFormat]),
        (json_io(), [DelimitedFileObjectFormat]),
        (delim_io(), [DelimitedFileObjectFormat]),
    ]
    for obj, formats in maybe_instances:
        for fmt in formats:
            assert fmt.maybe_instance(obj)


def test_not_definitely_is_instance():
    not_def_instances = [
        (at, [RecordsFormat, DataFrameIteratorFormat, RecordsIteratorFormat]),
        (
            df,
            [
                RecordsFormat,
                DataFrameIteratorFormat,
                RecordsIteratorFormat,
                ArrowTableFormat,
            ],
        ),
        (
            [],
            [
                DataFrameFormat,
                DataFrameIteratorFormat,
                RecordsIteratorFormat,
                ArrowTableFormat,
            ],
        ),
        ((d for d in range(10)), [DataFrameFormat, RecordsFormat, ArrowTableFormat]),
        (json_io(), [DelimitedFileObjectFormat]),
    ]
    for obj, formats in not_def_instances:
        for fmt in formats:
            assert not fmt.definitely_instance(obj)


def test_not_maybe_is_instance():
    not_maybe_instances = [
        (
            df,
            [
                RecordsFormat,
                DataFrameIteratorFormat,
                RecordsIteratorFormat,
                ArrowTableFormat,
            ],
        ),
        (
            [],
            [
                DataFrameFormat,
                DataFrameIteratorFormat,
                RecordsIteratorFormat,
                ArrowTableFormat,
            ],
        ),
        ((d for d in range(10)), [DataFrameFormat, RecordsFormat]),
    ]
    for obj, formats in not_maybe_instances:
        for fmt in formats:
            assert not fmt.maybe_instance(obj)


records = [{"f1": "hi", "f2": 1}, {"f1": "bye", "f2": 2}]
rf = (RecordsFormat, lambda: records)
dff = (DataFrameFormat, lambda: pd.DataFrame.from_records(records))
atf = (
    ArrowTableFormat,
    lambda: pa.Table.from_pandas(pd.DataFrame.from_records(records)),
)
dlff = (DelimitedFileObjectFormat, lambda: StringIO("f1,f2\nhi,1\nbye,2"))
rif = (RecordsIteratorFormat, lambda: ([r] for r in records))
dfif = (DataFrameIteratorFormat, lambda: (pd.DataFrame([r]) for r in records))
from_formats = [rf, dff, dfif, rif, dlff]
to_formats = [rf, dff]


@pytest.mark.parametrize(
    "fmt, obj_fn",
    [
        rf,
        dff,
        atf,
        rif,
        dfif,
    ],
)
def test_schema_transation(fmt: DataFormat, obj_fn: Callable):
    st = SchemaTranslation({"f1": "t1"}, TestSchema4)
    translated = fmt.apply_schema_translation(st, obj_fn())
    r = fmt.get_records_sample(translated, n=2)
    translated_records = [{"t1": "hi", "f2": 1}, {"t1": "bye", "f2": 2}]
    assert r == translated_records
