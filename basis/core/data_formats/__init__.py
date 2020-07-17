from __future__ import annotations

import collections
from typing import Any, Dict, Iterable, Optional, Union

import pandas as pd
from sqlalchemy import types

from basis.core.data_formats.base import (
    DataFormat,
    DataFormatBase,
    MemoryDataFormatBase,
)
from basis.core.data_formats.data_frame import DataFrameFormat
from basis.core.data_formats.data_frame_generator import (
    DataFrameGenerator,
    DataFrameGeneratorFormat,
)
from basis.core.data_formats.database_cursor import DatabaseCursorFormat
from basis.core.data_formats.database_table import DatabaseTableFormat
from basis.core.data_formats.database_table_ref import (
    DatabaseTableRef,
    DatabaseTableRefFormat,
)
from basis.core.data_formats.delimited_file import DelimitedFileFormat
from basis.core.data_formats.delimited_file_pointer import DelimitedFilePointerFormat
from basis.core.data_formats.json_list_file import JsonListFileFormat
from basis.core.data_formats.records_list import RecordsList, RecordsListFormat
from basis.core.data_formats.records_list_generator import (
    RecordsListGenerator,
    RecordsListGeneratorFormat,
)


class DataFormatRegistry:
    _registry: Dict[str, DataFormat] = {}

    def register(self, fmt_cls: DataFormat):
        self._registry[self.get_key(fmt_cls)] = fmt_cls

    def get_key(self, fmt_cls: DataFormat):
        return fmt_cls.__name__

    def get(self, key: str) -> DataFormat:
        return self._registry.get(key)

    def all(self) -> Iterable[DataFormat]:
        return self._registry.values()

    def __getitem__(self, item: str) -> DataFormat:
        return self._registry[item]


data_format_registry = DataFormatRegistry()
core_data_formats_precedence = [
    ### Memory formats
    # Roughly ordered from most universal / "default" to least
    # Ordering used when inferring DataFormat from raw object and have ambiguous object (eg an empty list)
    RecordsListFormat,
    DataFrameFormat,
    DatabaseCursorFormat,
    DatabaseTableRefFormat,
    RecordsListGeneratorFormat,
    DataFrameGeneratorFormat,
    DelimitedFilePointerFormat,
    ### Non-memory formats (can't be concrete python objects)
    DelimitedFileFormat,
    JsonListFileFormat,
    DatabaseTableFormat,
]
for fmt in core_data_formats_precedence:
    data_format_registry.register(fmt)


def get_data_format_of_object(obj: Any) -> Optional[DataFormat]:
    maybes = []
    for m in data_format_registry.all():
        if not m.is_memory_format():
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


class DataFormatType(types.TypeDecorator):
    impl = types.Unicode

    def process_bind_param(self, value, dialect):
        if not issubclass(value, DataFormatBase):
            raise TypeError(value)
        return data_format_registry.get_key(value)

    def process_result_value(self, value, dialect):
        return data_format_registry[value]


def get_records_list_sample(
    obj: Union[pd.DataFrame, RecordsList, RecordsListGenerator, DataFrameGenerator],
    max_sample: int = 1000,
) -> Optional[RecordsList]:
    """Helper for getting a small records list sample (poor mans converter?)"""
    # TODO: is this the right way/place to do this?
    # TODO: implement max_sample everywhere...
    if isinstance(obj, list):
        return obj[:max_sample]
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")[:max_sample]
    if isinstance(obj, DataFrameGenerator):
        return get_records_list_sample(obj.get_one())
    if isinstance(obj, RecordsListGenerator):
        return obj.get_one()
    if isinstance(obj, collections.abc.Generator):
        raise TypeError("Generators must be `tee`d before being passed in")
    raise TypeError(obj)
