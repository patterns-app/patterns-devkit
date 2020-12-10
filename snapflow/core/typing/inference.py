from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import pandas as pd
from dateutil.parser import ParserError
from loguru import logger
from pandas import DataFrame, Series
from snapflow.core.data_formats import RecordsList
from snapflow.core.module import DEFAULT_LOCAL_MODULE
from snapflow.core.typing.schema import (
    DEFAULT_UNICODE_TEXT_TYPE,
    DEFAULT_UNICODE_TYPE,
    MAX_UNICODE_LENGTH,
    ConflictBehavior,
    Field,
    Schema,
    create_quick_field,
    create_quick_schema,
)
from snapflow.utils.common import (
    ensure_bool,
    ensure_date,
    ensure_datetime,
    ensure_time,
    is_datetime_str,
    rand_str,
    title_to_snake_case,
)
from snapflow.utils.data import is_nullish, read_json, records_list_as_dict_of_lists
from sqlalchemy import Table

if TYPE_CHECKING:
    from snapflow.db.api import DatabaseAPI


def get_highest_precedence_sa_type(types: List[str]) -> str:
    for t in type_dominance:
        if t in types:
            return t
    return types[0]


def dataframe_to_sqlalchemy_table():
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


# type_dominance = [
#     "JSON",
#     "UnicodeText",
#     "Unicode",
#     "DateTime",
#     "Numeric",
#     "Float",
#     "BigInteger",
#     "Boolean",
# ]


def infer_schema_fields_from_records(
    records: RecordsList, sample_size: int = 100
) -> List[Field]:
    records = get_sample(records, sample_size=sample_size)
    # df = pd.DataFrame(records)
    d = records_list_as_dict_of_lists(records)
    fields = []
    for s in d:
        # satype = pandas_series_to_sqlalchemy_type(df[s])
        # objs = [r.get(s) for r in records]
        satype2 = get_sqlalchemy_type_for_python_objects(d[s])
        # if satype != satype2:
        #     logger.warning(f"Differing for {s}", satype, satype2)
        #     # satype2 = get_highest_precedence_sa_type([satype, satype2])
        f = create_quick_field(s, satype2)
        fields.append(f)
    return fields


def infer_schema_from_records_list(records: RecordsList, **kwargs) -> Schema:
    fields = infer_schema_fields_from_records(records)
    return generate_auto_schema(fields, **kwargs)


def generate_auto_schema(fields, **kwargs) -> Schema:
    auto_name = "AutoSchema_" + rand_str(8)
    args = dict(
        name=auto_name,
        module_name=DEFAULT_LOCAL_MODULE.name,
        version="0",
        description="Automatically inferred schema",
        unique_on=[],
        implementations=[],
        on_conflict=ConflictBehavior("ReplaceWithNewer"),
        fields=fields,
    )
    args.update(kwargs)
    return Schema(**args)


def create_sa_table(dbapi: DatabaseAPI, table_name: str) -> Table:
    sa_table = Table(
        table_name,
        dbapi.get_sqlalchemy_metadata(),
        autoload=True,
        autoload_with=dbapi.get_engine(),
    )
    return sa_table


def infer_schema_from_db_table(
    dbapi: DatabaseAPI, table_name: str, **schema_kwargs
) -> Schema:
    from snapflow.core.sql.utils import fields_from_sqlalchemy_table

    fields = fields_from_sqlalchemy_table(create_sa_table(dbapi, table_name))
    return generate_auto_schema(fields, **schema_kwargs)


def dict_to_rough_schema(name: str, d: Dict, convert_to_snake_case=True, **kwargs):
    fields = []
    for k, v in d.items():
        if convert_to_snake_case:
            k = title_to_snake_case(k)
        fields.append((k, pandas_series_to_sqlalchemy_type(pd.Series([v]))))
    fields = sorted(fields)
    return create_quick_schema(name, fields, **kwargs)


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
        return "Float"  # TODO: precision? Float(precision=53)?
    elif dtype.lower().startswith("int"):
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
    elif dtype == "empty":
        return DEFAULT_UNICODE_TYPE
    else:
        # Handle object/string case
        if has_dict_or_list(series):
            return "JSON"
        try:
            # First test one value
            v = series.dropna().iloc[0]
            if is_datetime_str(v):
                # Now see if the whole series can parse without error
                pd.to_datetime(series)
                return "DateTime"
        except ParserError:
            pass
    return DEFAULT_UNICODE_TYPE


def sqlalchemy_type_to_pandas_type(satype: str) -> str:
    ft = satype.lower()
    if ft.startswith("datetime"):
        return "datetime64[ns]"
    if ft.startswith("date"):
        return "date"
    if ft.startswith("time"):
        return "time"
    if ft.startswith("float") or ft.startswith("real"):
        return "float64"
    if ft.startswith("numeric"):
        return "float64"  # TODO: Does np/pd support Decimal?
    if ft.startswith("double"):
        return "float64"
    # Note: numpy integers cannot express null/na, so we have to use float64 in general case?
    # FIXED: Can use pandas defined type Int64 (caps), that supports pandas.NA)
    if ft.startswith("integer"):
        return "Int32"
    if ft.startswith("bigint"):
        return "Int64"
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
type_dominance = [
    DEFAULT_UNICODE_TYPE,
    DEFAULT_UNICODE_TEXT_TYPE,
    "UnicodeText",
    "Unicode",
    "JSON",
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
        if len(o) > MAX_UNICODE_LENGTH:
            return DEFAULT_UNICODE_TEXT_TYPE
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
        str=DEFAULT_UNICODE_TYPE,
        int="BigInteger",
        float="Float",
        Decimal="Numeric",
        dict="JSON",
        list="JSON",
        bool="Boolean",
        datetime="DateTime",
        NoneType=DEFAULT_UNICODE_TYPE,
    ).get(type(o).__name__, DEFAULT_UNICODE_TYPE)


def get_sqlalchemy_type_for_python_objects(objects: Iterable[Any]) -> str:
    types = []
    for o in objects:
        typ = get_sqlalchemy_type_for_python_object(o)
        if typ is None:
            continue
        types.append(typ)
    if not types:
        # We detected no types, column is all null-like, or there is no data
        return DEFAULT_UNICODE_TYPE
    # try:
    #     mode_type = mode(types)
    # except StatisticsError:
    #     mode_type = None
    dom_type = get_highest_precedence_sa_type(list(set(types)))
    return dom_type


def cast_python_object_to_sqlalchemy_type(obj: Any, satype: str) -> Any:
    if obj is None:
        return None
    if pd.isna(obj):
        return None
    ft = satype.lower()
    if ft.startswith("datetime"):
        return ensure_datetime(obj)
    if ft.startswith("date"):
        return ensure_date(obj)
    if ft.startswith("time"):
        return ensure_time(obj)
    if ft.startswith("float") or ft.startswith("real"):
        return float(obj)
    if ft.startswith("numeric"):
        return Decimal(obj)
    if ft.startswith("integer"):
        return int(obj)
    if ft.startswith("biginteger"):
        return int(obj)
    if ft.startswith("boolean"):
        return ensure_bool(obj)
    if (
        ft.startswith("string")
        or ft.startswith("unicode")
        or ft.startswith("varchar")
        or ft.startswith("text")
    ):
        return str(obj)
    if ft.startswith("json"):
        if isinstance(obj, str):
            return read_json(obj)
        else:
            return obj
    raise NotImplementedError


def conform_records_list_to_schema(d: RecordsList, schema: Schema) -> RecordsList:
    conformed = []
    for r in d:
        new_record = {}
        for k, v in r.items():
            new_v = cast_python_object_to_sqlalchemy_type(
                v, schema.get_field(k).field_type
            )
            new_record[k] = new_v
        conformed.append(new_record)
    return conformed


def conform_dataframe_to_schema(df: DataFrame, schema: Schema) -> DataFrame:
    logger.debug(f"conforming {df.head()} to schema {schema}")
    for field in schema.fields:
        pd_type = sqlalchemy_type_to_pandas_type(field.field_type)
        try:
            if field.name in df:
                if df[field.name].dtype.name == pd_type:
                    continue
                # TODO: `astype` is not aggressive enough (won't cast values), so doesn't work
                # Likely need combo of "hard" conversion using `to_*` methods and `infer_objects`
                # and explicit individual python casts if that fails
                if "datetime" in pd_type:
                    logger.debug(f"Casting {field.name} to datetime")
                    df[field.name] = pd.to_datetime(df[field.name])
                else:
                    try:
                        df[field.name] = df[field.name].astype(pd_type)
                        logger.debug(f"Casting {field.name} to {pd_type}")
                    except (TypeError, ValueError, ParserError):
                        logger.debug(
                            f"Manually casting {field.name} to py objects {field.field_type}"
                        )
                        df[field.name] = [
                            cast_python_object_to_sqlalchemy_type(v, field.field_type)
                            for v in df[field.name]
                        ]
            else:
                df[field.name] = Series(dtype=pd_type)
        except Exception as e:
            raise e
    return df
