from __future__ import annotations

from dataclasses import asdict
from typing import Any, List, Tuple

import pandas as pd
import pytest

from snapflow.core.data_formats import (
    DatabaseCursorFormat,
    DataFormatBase,
    DataFrameFormat,
    DataFrameGenerator,
    DataFrameGeneratorFormat,
    RecordsListFormat,
    RecordsListGenerator,
    RecordsListGeneratorFormat,
    get_records_list_sample,
)
from tests.utils import sample_records


def test_definitely_is_instance():
    df = pd.DataFrame({"a": range(10)})
    definitely_instances = [
        (df, [DataFrameFormat]),
        (DataFrameGenerator(d for d in [df]), [DataFrameGeneratorFormat]),
        ([{}, {}], [RecordsListFormat]),
        (RecordsListGenerator(r for r in [{}, {}]), [RecordsListGeneratorFormat]),
    ]
    for obj, formats in definitely_instances:
        for fmt in formats:
            assert fmt.definitely_instance(obj)


def test_maybe_is_instance():
    df = pd.DataFrame({"a": range(10)})
    maybe_instances = [
        (df, [DataFrameFormat]),
        (
            (d for d in range(10)),
            [DataFrameGeneratorFormat, RecordsListGeneratorFormat],
        ),
        ([], [RecordsListFormat]),
    ]
    for obj, formats in maybe_instances:
        for fmt in formats:
            assert fmt.maybe_instance(obj)


def test_not_is_instance():
    df = pd.DataFrame({"a": range(10)})
    maybe_instances = [
        (df, [RecordsListFormat, DataFrameGeneratorFormat, RecordsListGeneratorFormat]),
        ([], [DataFrameFormat, DataFrameGeneratorFormat, RecordsListGeneratorFormat]),
        ((d for d in range(10)), [DataFrameFormat, RecordsListFormat]),
    ]
    for obj, formats in maybe_instances:
        for fmt in formats:
            assert not fmt.maybe_instance(obj)
            assert not fmt.definitely_instance(obj)


def test_get_records_list_sample():
    n = 100
    records = [{}] * 1000
    sample = get_records_list_sample(records, max_sample=n)
    assert len(sample) == n
    records = pd.DataFrame({"a": range(1000)})
    sample = get_records_list_sample(records, max_sample=n)
    assert len(sample) == n
