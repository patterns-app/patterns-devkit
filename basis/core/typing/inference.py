from __future__ import annotations

import logging
from random import randint
from statistics import StatisticsError, mode
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import pandas as pd
from pandas import Series
from sqlalchemy import Table

from basis.core.component import ComponentType
from basis.core.data_format import RecordsList
from basis.core.module import DEFAULT_LOCAL_MODULE
from basis.core.typing.object_type import (
    ConflictBehavior,
    Field,
    ObjectType,
    create_quick_field,
    create_quick_otype,
)
from basis.utils.common import is_datetime_str, title_to_snake_case
from basis.utils.data import is_nullish, records_list_as_listdicts

if TYPE_CHECKING:
    from basis.db.api import DatabaseAPI

logger = logging.getLogger(__name__)


"""
We piggy back on pandas tools here for converting data to pandas/numpy
dtypes and then dtypes to sqlalchemy column types.
"""
# TODO: This needs to be redone. Prioritize compatible with SQL over Numpy.
#    For instance, SQL handles nulls in all types, whereas numpy can only sentinel NaN in floats, not in ints.


VARCHAR_MAX_LEN = (
    256  # TODO: what is the real value for different db engines? pg is NA, mysql ??
)


def get_highest_precedence_sa_type(types: List[str]) -> str:
    for t in type_precendence:
        if t in types:
            return t
    return types[0]


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


# type_precendence = [
#     "JSON",
#     "UnicodeText",
#     "Unicode",
#     "DateTime",
#     "Numeric",
#     "Float",
#     "BigInteger",
#     "Boolean",
# ]


def infer_otype_fields_from_records(
    records: RecordsList, sample_size: int = 100
) -> List[Field]:
    records = get_sample(records, sample_size=sample_size)
    # df = pd.DataFrame(records)
    d = records_list_as_listdicts(records)
    fields = []
    for s in d:
        # satype = pandas_series_to_sqlalchemy_type(df[s])
        # objs = [r.get(s) for r in records]
        satype2 = get_sqlalchemy_type_for_python_objects(d[s])
        # if satype != satype2:
        #     print(f"Differing for {s}", satype, satype2)
        #     # satype2 = get_highest_precedence_sa_type([satype, satype2])
        f = create_quick_field(s, satype2)
        fields.append(f)
    return fields


def infer_otype_from_records_list(records: RecordsList, **kwargs) -> ObjectType:
    fields = infer_otype_fields_from_records(records)
    return generate_auto_otype(fields, **kwargs)


def generate_auto_otype(fields, **kwargs) -> ObjectType:
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


def create_sa_table(dbapi: DatabaseAPI, table_name: str) -> Table:
    sa_table = Table(
        table_name,
        dbapi.get_sqlalchemy_metadata(),
        autoload=True,
        autoload_with=dbapi.get_engine(),
    )
    return sa_table


def infer_otype_from_db_table(
    dbapi: DatabaseAPI, table_name: str, **otype_kwargs
) -> ObjectType:
    from basis.core.sql.utils import fields_from_sqlalchemy_table

    fields = fields_from_sqlalchemy_table(create_sa_table(dbapi, table_name))
    return generate_auto_otype(fields, **otype_kwargs)


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
        return "datetime64[ns]"
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


# NB: list is a bit counter-intuitive. since types are converted as aggressively as possible,
# higher precedence here means *less specific* types, since there were some values we couldn't
# convert to more specific type.
type_precendence = [
    "JSON",  # JSON is least specific type, can handle most sub-types as special case
    "UnicodeText",
    "Unicode",
    "DateTime",  # The way we convert datetimes is pretty aggressive, so this could lead to false positives
    "Numeric",
    "Float",
    "BigInteger",
    "Boolean",  # bool is most specific, can only handle 0/1, nothing else
]


def get_sqlalchemy_type_for_python_object(o: Any) -> Optional[str]:
    # This is a: Dirty filthy good for nothing hack
    # Defaults to unicode text
    if is_nullish(o):
        return None
    if isinstance(o, str):
        # Try some things with str and see what sticks
        if len(o) > VARCHAR_MAX_LEN:
            return "UnicodeText"
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
        str="Unicode",
        int="BigInteger",
        float="Float",
        Decimal="Numeric",
        dict="JSON",
        list="JSON",
        bool="Boolean",
        datetime="DateTime",
        NoneType="UnicodeText",
    ).get(type(o).__name__, "UnicodeText")


def get_sqlalchemy_type_for_python_objects(objects: Iterable[Any]) -> str:
    types = []
    for o in objects:
        typ = get_sqlalchemy_type_for_python_object(o)
        if typ is None:
            continue
        types.append(typ)
    try:
        mode_type = mode(types)
    except StatisticsError:
        mode_type = None
    dom_type = get_highest_precedence_sa_type(list(set(types)))
    # print(f"Mode {mode_type} Dom {dom_type}")
    return dom_type


# TODO: any point to this? just adding missing columns as None. Type conversion is real reason, todo i guess
# def coerce_records_list_to_otype(d: RecordsList, otype: ObjectType) -> RecordsList:
#     # sa_cols = ObjectTypeMapper(env).to_sqlalchemy(otype)
#     for field in otype.fields:
#         pd_type = field_type_to_pandas_type(field.field_type)
#         if field.name in df:
#             df[field.name] = df[field.name].astype(pd_type, copy=False)
#         else:
#             df[field.name] = pd.Series(dtype=pd_type)
#     return df
