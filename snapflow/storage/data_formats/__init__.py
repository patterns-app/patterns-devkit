from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
from snapflow.storage.data_formats.base import (
    DataFormat,
    DataFormatBase,
    MemoryDataFormatBase,
    SampleableIterator,
)
from snapflow.storage.data_formats.data_frame import (
    DataFrameFormat,
    DataFrameIterator,
    DataFrameIteratorFormat,
)
from snapflow.storage.data_formats.database_cursor import DatabaseCursorFormat
from snapflow.storage.data_formats.database_table import DatabaseTableFormat
from snapflow.storage.data_formats.database_table_ref import (
    DatabaseTableRef,
    DatabaseTableRefFormat,
)
from snapflow.storage.data_formats.delimited_file import DelimitedFileFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
    DelimitedFileObjectIterator,
    DelimitedFileObjectIteratorFormat,
)
from snapflow.storage.data_formats.json_lines_file import JsonLinesFileFormat
from snapflow.storage.data_formats.records import (
    Records,
    RecordsFormat,
    RecordsIterator,
    RecordsIteratorFormat,
)
from snapflow.utils.registry import global_registry
from sqlalchemy import types

# data_format_registry = ClassRegistry()
core_data_formats_precedence: List[DataFormat] = [
    ### Memory formats
    # Roughly ordered from most universal / "default" to least
    # Ordering used when inferring DataFormat from raw object and have ambiguous object (eg an empty list)
    RecordsFormat,
    DataFrameFormat,
    DatabaseCursorFormat,
    DatabaseTableRefFormat,
    RecordsIteratorFormat,
    DataFrameIteratorFormat,
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
    ### Non-python formats (can't be concrete python objects)
    DelimitedFileFormat,
    JsonLinesFileFormat,
    DatabaseTableFormat,
]
for fmt in core_data_formats_precedence:
    global_registry.register(fmt)


def register_format(format: DataFormat):
    global_registry.register(format)


def get_data_format_of_object(obj: Any) -> Optional[DataFormat]:
    maybes = []
    for m in global_registry.all(DataFormatBase):
        if not m.is_python_format():
            continue
        assert issubclass(m, MemoryDataFormatBase)
        try:
            if m.definitely_instance(obj):
                return m
        except NotImplementedError:
            pass
        if m.maybe_instance(obj):
            maybes.append(m)
    if len(maybes) == 1:
        return maybes[0]
    elif len(maybes) > 1:
        return [f for f in core_data_formats_precedence if f in maybes][0]
    return None


# Deprecated formats
RecordsGenerator = RecordsIterator
DataFrameGenerator = DataFrameIterator
