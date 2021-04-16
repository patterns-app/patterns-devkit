# from __future__ import annotations

# import traceback
# from collections.abc import Iterable
# from decimal import Decimal
# from typing import TYPE_CHECKING, Any, Dict, List, Optional

# import pandas as pd
# import pyarrow as pa
# from dateutil.parser import ParserError
# from loguru import logger
# from pandas import DataFrame, Series
# from snapflow.core.module import DEFAULT_LOCAL_MODULE


#     fields_from_sqlalchemy_table,
#     infer_field_types_from_records,
#     infer_fields_from_dataframe,
#     infer_fields_from_records,
#     pandas_series_to_field_type,
# )


# from dcp.utils.common import (
#     ensure_bool,
#     ensure_date,
#     ensure_datetime,
#     ensure_time,
#     is_datetime_str,
#     rand_str,
#     title_to_snake_case,
# )
# from snapflow.utils.data import is_nullish, read_json, records_as_dict_of_lists
# from sqlalchemy import Table

# if TYPE_CHECKING:


# def dataframe_to_sqlalchemy_table():
#     # TODO
#     # SQLTable()._get_column_names_and_types(self._sqlalchemy_type) # see pandas.io.sql
#     pass


# def infer_schema_from_records(records: Records, **kwargs) -> Schema:
#     fields = infer_fields_from_records(records)
#     return generate_auto_schema(fields, **kwargs)


# def generate_auto_schema(fields, **kwargs) -> Schema:
#     auto_name = "AutoSchema_" + rand_str(8)
#     args = dict(
#         name=auto_name,
#         namespace=DEFAULT_LOCAL_MODULE.name,
#         version="0",
#         description="Automatically inferred schema",
#         unique_on=[],
#         implementations=[],
#         fields=fields,
#     )
#     args.update(kwargs)
#     return Schema(**args)


# def create_sa_table(dbapi: DatabaseApi, table_name: str) -> Table:
#     sa_table = Table(
#         table_name,
#         dbapi.get_sqlalchemy_metadata(),
#         autoload=True,
#         autoload_with=dbapi.get_engine(),
#     )
#     return sa_table


# def infer_schema_from_db_table(
#     dbapi: DatabaseApi, table_name: str, **schema_kwargs
# ) -> Schema:
#     tble = create_sa_table(dbapi, table_name)
#     fields = fields_from_sqlalchemy_table(tble)
#     return generate_auto_schema(fields, **schema_kwargs)


# def dict_to_rough_schema(name: str, d: Dict, convert_to_snake_case=True, **kwargs):
#     fields = []
#     for k, v in d.items():
#         if convert_to_snake_case:
#             k = title_to_snake_case(k)
#         fields.append((k, pandas_series_to_field_type(pd.Series([v]))))
#     fields = sorted(fields)
#     return create_quick_schema(name, fields, **kwargs)


# def conform_records_to_schema(d: Records, schema: Schema) -> Records:
#     # TODO: support cast levels
#     conformed = []
#     for r in d:
#         new_record = {}
#         for k, v in r.items():
#             if k in schema.field_names():
#                 v = cast_python_object_to_field_type(v, schema.get_field(k).field_type)
#             new_record[k] = v
#         conformed.append(new_record)
#     return conformed


# def infer_schema_from_dataframe(df: DataFrame, **kwargs) -> Schema:
#     fields = infer_fields_from_dataframe(df)
#     return generate_auto_schema(fields, **kwargs)


# def conform_dataframe_to_schema(df: DataFrame, schema: Schema) -> DataFrame:
#     # TODO: support cast levels
#     logger.debug(f"conforming {df.head(5)} to schema {schema}")
#     for field in schema.fields:
#         pd_type = field.field_type.pandas_type
#         try:
#             if field.name in df:
#                 if df[field.name].dtype.name == pd_type:
#                     continue
#                 # TODO: `astype` is not aggressive enough (won't cast values), so doesn't work
#                 # Likely need combo of "hard" conversion using `to_*` methods and `infer_objects`
#                 # and explicit individual python casts if that fails
#                 if "datetime" in pd_type:
#                     logger.debug(f"Casting {field.name} to datetime")
#                     df[field.name] = pd.to_datetime(df[field.name])
#                 else:
#                     try:
#                         df[field.name] = df[field.name].astype(pd_type)
#                         logger.debug(f"Casting {field.name} to {pd_type}")
#                     except (TypeError, ValueError, ParserError):
#                         logger.debug(
#                             f"Manually casting {field.name} to py objects {field.field_type}"
#                         )
#                         df[field.name] = [
#                             cast_python_object_to_field_type(v, field.field_type)
#                             for v in df[field.name]
#                         ]
#             else:
#                 df[field.name] = Series(dtype=pd_type)
#         except Exception as e:
#             raise e
#     return df


# def conform_arrow_to_schema(at: ArrowTable, schema: Schema) -> ArrowTable:
#     # TODO: no conforming, just checking the types match
#     for field in schema.fields:
#         af = at.schema.field(field.name)
#         if af is None:
#             # TODO: add empty column to table
#             continue
#         assert field.field_type == ensure_field_type(str(af.type))
#     return at
