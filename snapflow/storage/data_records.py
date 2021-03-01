from __future__ import annotations

from collections import abc
from dataclasses import dataclass
from io import IOBase
from typing import TYPE_CHECKING, Any, Callable, Generic, Optional, Tuple, Type

from pandas.core.frame import DataFrame
from snapflow.schema.base import Schema, SchemaLike
from snapflow.utils.data import (
    SampleableCursor,
    SampleableGenerator,
    SampleableIO,
    SampleableIterator,
)
from sqlalchemy.engine.result import ResultProxy

if TYPE_CHECKING:
    from snapflow.storage.data_formats import DataFormat


@dataclass
class MemoryDataRecords:
    # TODO: different name for this class?
    records_object: Any
    _raw_records_object: Any
    _data_format: Optional[DataFormat] = None
    _record_count: Optional[int] = None
    # _closeable_resource: Optional[Any] = none  # TODO: when / how to use?
    nominal_schema: Optional[SchemaLike] = None
    closeable: Optional[Callable] = None

    @property
    def data_format(self) -> DataFormat:
        from snapflow.storage.data_formats import get_data_format_of_object

        if self._data_format:
            return self._data_format
        data_format = get_data_format_of_object(self.records_object)
        if data_format is None:
            raise NotImplementedError(
                f"Encountered unrecognized data format: {self.records_object}"
            )
        assert data_format.is_python_format()
        self._data_format = data_format
        return self._data_format

    @property
    def record_count(self) -> Optional[int]:
        if self._record_count is not None:
            return self._record_count
        record_count = self.data_format.get_record_count(self.records_object)
        self._record_count = record_count
        return self._record_count

    def copy(self) -> MemoryDataRecords:
        wrapped_records_object = self.data_format.copy_records(self.records_object)
        return MemoryDataRecords(
            _raw_records_object=self._raw_records_object,
            records_object=wrapped_records_object,
            _data_format=self._data_format,
            _record_count=self._record_count,
            nominal_schema=self.nominal_schema,
        )

    @property
    def record_count_display(self):
        return self.record_count if self.record_count is not None else "Unknown"

    def conform_to_schema(self, schema: Schema = None) -> MemoryDataRecords:
        if schema is None:
            assert isinstance(self.nominal_schema, Schema)
            schema = self.nominal_schema
        records_obj = self.data_format.conform_records_to_schema(
            self.records_object, schema
        )
        return MemoryDataRecords(
            _raw_records_object=records_obj,
            records_object=wrap_records_object(records_obj),
            _data_format=self._data_format,
            _record_count=self._record_count,
            nominal_schema=schema,
        )


def as_records(
    records_object: Any,
    data_format: DataFormat = None,
    record_count: int = None,
    schema: SchemaLike = None,
) -> MemoryDataRecords:
    if isinstance(records_object, MemoryDataRecords):
        # No nesting
        return records_object
    mdr = MemoryDataRecords(
        records_object=wrap_records_object(records_object),
        _raw_records_object=records_object,
        _data_format=data_format,
        _record_count=record_count,
        nominal_schema=schema,
    )
    return mdr


def wrap_records_object(obj: Any) -> Any:
    """
    Wrap records object that are exhaustable (eg generators, file objects, db cursors)
    so that we can sample them for inspection and inference without losing records.
    """
    if isinstance(obj, SampleableIterator):
        # Already wrapped
        return obj
    if isinstance(obj, ResultProxy):
        return SampleableCursor(obj)
    if isinstance(obj, IOBase):
        return SampleableIO(obj)
    if isinstance(obj, abc.Generator):
        return SampleableGenerator(obj)
    if isinstance(obj, abc.Iterator):
        return SampleableIterator(obj)
    return obj


def is_records_generator(obj: Any) -> bool:
    if isinstance(obj, DataFrame):
        return False
    if isinstance(obj, list):
        # TODO: could see a snap returning e.g. a list of DataFrames and thinking that would be valid
        return False
    if isinstance(obj, abc.Generator):
        return True
    return False


def records_object_is_definitely_empty(obj: Any) -> bool:
    if isinstance(obj, list) or isinstance(obj, DataFrame):
        return len(obj) == 0
    if isinstance(obj, SampleableIterator):
        return obj.get_first() is None
    return False
