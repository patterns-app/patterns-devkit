from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from random import randint
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from pandas import Series

from basis.core.component import ComponentType
from basis.core.data_format import DictList
from basis.core.module import DEFAULT_LOCAL_MODULE
from basis.core.typing.object_type import (
    Field,
    ObjectType,
    create_quick_field,
    create_quick_otype,
    ConflictBehavior,
)
from basis.utils.common import is_datetime_str, title_to_snake_case

logger = logging.getLogger(__name__)


"""
We piggy back on pandas tools here for converting data to pandas/numpy
dtypes and then dtypes to sqlalchemy column types.
"""
# TODO: This needs to be redone. Prioritize compatible with SQL over Numpy.
#    For instance, SQL handles nulls in all types, whereas numpy can only sentinel NaN in floats, not in ints.


def dataframe_to_sqlalchemy_schema():
    # TODO
    # SQLTable()._get_column_names_and_types(self._sqlalchemy_type) # see pandas.io.sql
    pass


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


def infer_otype_fields_from_records(
    records: DictList, sample_size: int = 100
) -> List[Field]:
    records = get_sample(records, sample_size=sample_size)
    df = pd.DataFrame(records)
    fields = []
    for s in df:
        satype = pandas_series_to_sqlalchemy_type(df[s])
        f = create_quick_field(s, satype)
        fields.append(f)
    return fields


def infer_otype(records: DictList, **kwargs) -> ObjectType:
    fields = infer_otype_fields_from_records(records)
    auto_name = "AutoType" + str(randint(1000, 9999))  # TODO
    args = dict(
        component_type=ComponentType.ObjectType,
        name=auto_name,
        module_name=DEFAULT_LOCAL_MODULE.name,
        version="0",
        description=f"Automatically inferred type",
        unique_on=[],
        implementations=[],
        on_conflict=ConflictBehavior("ReplaceWithNewer"),
        fields=fields,
    )
    args.update(kwargs)
    return ObjectType(**args)


def dict_to_rough_otype(name: str, d: Dict, convert_to_snake_case=True, **kwargs):
    fields = []
    for k, v in d.items():
        if convert_to_snake_case:
            k = title_to_snake_case(k)
        fields.append((k, pandas_series_to_sqlalchemy_type(pd.Series([v]))))
    fields = sorted(fields)
    return create_quick_otype(name, fields, **kwargs)


def has_dict_or_list(series: Series) -> bool:
    return any(series.apply(lambda x: isinstance(x, dict) or isinstance(x, list)))


def pandas_series_to_sqlalchemy_type(series: Series) -> str:
    """
    Cribbed from pandas.io.sql
    Changes:
        - strict datetime and JSON inference
        - No timezone handling
        - No single/32 numeric types 
    """
    dtype = pd.api.types.infer_dtype(series, skipna=True)
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
        return "DateTime"
    if dtype == "timedelta64":
        raise NotImplementedError  # TODO
    elif dtype == "floating":
        # if col.dtype == "float32":
        #     return Float(precision=23)
        # else:
        #     return Float(precision=53
        "Float"  # TODO: precision? Float(precision=53)?
    elif dtype == "integer":
        # if col.dtype == "int32":
        #     return Integer
        # else:
        #     return BigInteger
        return "BigInteger"  # TODO: size
    elif dtype == "boolean":
        return "Boolean"
    elif dtype == "date":
        return "Date"
    elif dtype == "time":
        return "Time"
    elif dtype == "complex":
        raise ValueError("Complex datatypes not supported")
    else:
        # Handle object/string case
        if has_dict_or_list(series):
            return "JSON"
        try:
            pd.to_datetime(series)
            return "DateTime"
        except:
            pass
    return "UnicodeText"


def sqlalchemy_type_to_pandas_type(satype: str) -> str:
    ft = satype.lower()
    if ft.startswith("datetime"):
        return "datetime64"
    if ft.startswith("date"):
        return "date"
    if ft.startswith("time"):
        return "time"
    if ft.startswith("float"):
        return "float64"
    if ft.startswith("numeric"):
        return "float64"  # TODO: Does np/pd support Decimal?
    if ft.startswith("integer"):
        return "int32"
    if ft.startswith("biginteger"):
        return "int64"
    if ft.startswith("boolean"):
        return "boolean"
    if (
        ft.startswith("string")
        or ft.startswith("unicode")
        or ft.startswith("varchar")
        or ft.startswith("text")
    ):
        return "string"
    if ft.startswith("json"):
        return "object"
    raise NotImplementedError


# type_precendence = [
#     "Numeric",
#     "Float",
#     "JSON",
#     "DateTime",
#     "BigInteger",
#     "Boolean",
#     "Unicode",
# ]


def get_sqlalchemy_type_for_python_object(o: Any) -> str:
    # This is a: Dirty filthy good for nothing hack
    # Defaults to unicode
    if isinstance(o, str):
        # Try some things with str and see what sticks
        if is_datetime_str(o):
            return "DateTime"
        try:
            o = int(o)
        except ValueError:
            try:
                o = float(o)
            except ValueError:
                pass
    return dict(
        str="UnicodeText",
        int="BigInteger",
        float="Float",
        Decimal="Numeric",
        dict="JSON",
        list="JSON",
        bool="Boolean",
        datetime="DateTime",
        NoneType="UnicodeText",
    ).get(type(o).__name__, "UnicodeText")


# def get_sqlalchemy_type_for_python_objects(objects: Iterable[Any]) -> str:
#     types = set([])
#     for o in objects:
#         typ = get_sqlalchemy_type_for_python_object(o)
#         types.add(typ)
#     for t in type_precendence:
#         if t in types:
#             return t
#     raise Exception("Shouldn't get here")
