from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set, Type

import pandas as pd
from loguru import logger
from pandas.core.series import Series
from snapflow.schema.base import Field, Schema
from snapflow.schema.field_types import (
    DEFAULT_FIELD_TYPE,
    LONG_TEXT,
    Boolean,
    Date,
    DateTime,
    Decimal,
    FieldType,
    FieldTypeBase,
    Float,
    Integer,
    Json,
    LongText,
    Text,
    Time,
    all_types,
    all_types_instantiated,
    ensure_field_type,
)
from snapflow.storage.data_formats.records import Records
from snapflow.utils.data import is_nullish, records_as_dict_of_lists
from snapflow.utils.registry import ClassBasedEnum, global_registry
from sqlalchemy.sql.schema import Column, Table


def get_sample(
    values: List[Any], sample_size: int = 100, method: str = "headtail"
) -> List[Any]:
    if method != "headtail":
        raise NotImplementedError
    if len(values) < sample_size:
        sample = values
    else:
        half = sample_size // 2
        sample = values[:half]
        sample += values[-half:]
    return sample


def _detect_field_type_fast(obj: Any) -> Optional[FieldType]:
    """
    Fast, but doesn't support adding new types via the registry.
    TODO: Fixable tho, just need to make sure added types are ranked by cardinality (separate registry?)
    """
    if is_nullish(obj):
        # TODO: this is pretty aggressive?
        return None
    for ft in all_types_instantiated:
        if ft.is_definitely(obj):
            return ft
    for ft in all_types_instantiated:
        if ft.is_maybe(obj):
            return ft
    # I don't think we should get here ever? Some random object type
    logger.error(obj)
    return DEFAULT_FIELD_TYPE


def _detect_field_type_complete(obj: Any) -> Optional[FieldType]:
    if is_nullish(obj):
        # TODO: this is pretty aggressive?
        return None
    # If we have an
    definitelys = []
    for ft in global_registry.all(FieldTypeBase):
        if isinstance(ft, type):
            ft = ft()
        if ft.is_definitely(obj):
            definitelys.append(ft)
    if definitelys:
        # Take lowest cardinality definitely
        return min(definitelys, key=lambda x: x.cardinality_rank)
    maybes = []
    for ft in global_registry.all(FieldTypeBase):
        if isinstance(ft, type):
            ft = ft()
        if ft.is_maybe(obj):
            maybes.append(ft)
    if not maybes:
        # I don't think we should get here ever? Some random object type
        logger.error(obj)
        return DEFAULT_FIELD_TYPE
    # Take lowest cardinality maybe
    return min(maybes, key=lambda x: x.cardinality_rank)


detect_field_type = _detect_field_type_fast


def select_field_type(objects: Iterable[Any]) -> FieldType:
    types = set()
    for o in objects:
        # Choose the minimum compatible type
        typ = detect_field_type(o)
        if typ is None:
            continue
        types.add(typ)
    if not types:
        # We detected no types, column is all null-like, or there is no data
        logger.warning("No field types detected")
        return DEFAULT_FIELD_TYPE
    # Now we must choose the HIGHEST cardinality, to accomodate ALL values
    # (the maximum minimum type)
    return max(types, key=lambda x: x.cardinality_rank)


def infer_field_types_from_records(
    records: Records, sample_size: int = 100
) -> Dict[str, FieldType]:
    records = get_sample(records, sample_size=sample_size)
    d = records_as_dict_of_lists(records)
    fields = {}
    for s in d:
        ft = select_field_type(d[s])
        fields[s] = ft
    return fields


def infer_fields_from_records(records: Records, **kwargs) -> List[Field]:
    field_types = infer_field_types_from_records(records)
    fields = [Field(name=k, field_type=v) for k, v in field_types.items()]
    return fields


def pandas_series_to_field_type(series: Series) -> FieldType:
    """
    Cribbed from pandas.io.sql
    Changes:
        - strict datetime and JSON inference
        - No timezone handling
        - No single/32 numeric types
    """
    dtype = pd.api.types.infer_dtype(series, skipna=True).lower()
    if dtype == "datetime64" or dtype == "datetime":
        # GH 9086: TIMESTAMP is the suggested type if the column contains
        # timezone information
        # try:
        #     if col.dt.tz is not None:
        #         return TIMESTAMP(timezone=True)
        # except AttributeError:
        #     # The column is actually a DatetimeIndex
        #     if col.tz is not None:
        #         return TIMESTAMP(timezone=True)
        return DateTime()
    if dtype == "timedelta64":
        raise NotImplementedError  # TODO
    elif dtype.startswith("float"):
        return Float()
    elif dtype.startswith("int"):
        return Integer()
    elif dtype == "boolean":
        return Boolean()
    elif dtype == "date":
        return Date()
    elif dtype == "time":
        return Time()
    elif dtype == "complex":
        raise ValueError("Complex number datatype not supported")
    elif dtype == "empty":
        return DEFAULT_FIELD_TYPE
    else:
        # Handle object / string case as generic detection
        # try:
        return select_field_type(series.dropna().iloc[:100])
        # except ParserError:
        #     pass
    return DEFAULT_FIELD_TYPE


def infer_fields_from_dataframe(df: pd.DataFrame, **kwargs) -> List[Field]:
    fields = []
    for c in df.columns:
        ft = pandas_series_to_field_type(df[c])
        fields.append(Field(name=c, field_type=ft))
    return fields


def field_from_sqlalchemy_column(sa_column: Column, **kwargs) -> Field:
    # TODO: clean up reflected types? Conform / cast to standard set?
    return Field(
        name=sa_column.name, field_type=ensure_field_type(sa_column.type), **kwargs
    )


def fields_from_sqlalchemy_table(sa_table: Table) -> List[Field]:
    fields = []
    for column in sa_table.columns:
        fields.append(field_from_sqlalchemy_column(column))
    return fields
