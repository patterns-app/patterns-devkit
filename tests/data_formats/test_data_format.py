from __future__ import annotations

from dataclasses import asdict
from io import StringIO
from typing import Any, List, Tuple

import pandas as pd
import pytest
from snapflow.core.data_formats import (
    DatabaseCursorFormat,
    DataFormatBase,
    DataFrameFormat,
    DataFrameIterator,
    DataFrameIteratorFormat,
    RecordsListFormat,
    RecordsListIterator,
    RecordsListIteratorFormat,
)
from snapflow.core.data_formats.base import SampleableIterator
from snapflow.core.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
)
from tests.utils import sample_records

# Example formats
df = pd.DataFrame({"a": range(10)})


def delim_io():
    return StringIO("f1,f2,f3\n1,2,3")


def json_io():
    return StringIO('{"f1":1, "f2":2}\n{"f1":1, "f2":2}')  # TODO


def test_definitely_is_instance():
    definitely_instances = [
        (df, [DataFrameFormat]),
        (SampleableIterator(d for d in [df]), [DataFrameIteratorFormat]),
        ([{}, {}], [RecordsListFormat]),
        (SampleableIterator(r for r in [[{}], [{}]]), [RecordsListIteratorFormat]),
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
        (df, [DataFrameFormat]),
        (
            (d for d in range(10)),
            [
                DataFrameIteratorFormat,
                RecordsListIteratorFormat,
                DelimitedFileObjectIteratorFormat,
            ],
        ),
        ([], [RecordsListFormat]),
        (json_io(), [DelimitedFileObjectFormat]),
        (delim_io(), [DelimitedFileObjectFormat]),
    ]
    for obj, formats in maybe_instances:
        for fmt in formats:
            assert fmt.maybe_instance(obj)


def test_not_definitely_is_instance():
    maybe_instances = [
        (df, [RecordsListFormat, DataFrameIteratorFormat, RecordsListIteratorFormat]),
        ([], [DataFrameFormat, DataFrameIteratorFormat, RecordsListIteratorFormat]),
        ((d for d in range(10)), [DataFrameFormat, RecordsListFormat]),
        (json_io(), [DelimitedFileObjectFormat]),
    ]
    for obj, formats in maybe_instances:
        for fmt in formats:
            assert not fmt.definitely_instance(obj)


def test_not_maybe_is_instance():
    maybe_instances = [
        (df, [RecordsListFormat, DataFrameIteratorFormat, RecordsListIteratorFormat]),
        ([], [DataFrameFormat, DataFrameIteratorFormat, RecordsListIteratorFormat]),
        ((d for d in range(10)), [DataFrameFormat, RecordsListFormat]),
    ]
    for obj, formats in maybe_instances:
        for fmt in formats:
            assert not fmt.maybe_instance(obj)


# def test_get_records_list_sample():
#     n = 100
#     records = [{}] * 1000
#     sample = get_records_list_sample(records, max_sample=n)
#     assert len(sample) == n
#     records = pd.DataFrame({"a": range(1000)})
#     sample = get_records_list_sample(records, max_sample=n)
#     assert len(sample) == n
