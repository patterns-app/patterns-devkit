from typing import Any, List

import pandas as pd
from pandas import DataFrame, Series

from basis.core.data_format import DictList
from basis.core.typing.object_type import ObjectType

# def coerce_dataframe_to_otype(df: DataFrame, otype: ObjectType):
#     # sa_cols = ObjectTypeMapper(env).to_sqlalchemy(otype)
#     for field in otype.fields:
#         pd_type = field_type_to_pandas_type(field.field_type)
#         try:
#             if field.name in df:
#                 if df[field.name].dtype.name == pd_type:
#                     continue
#                 df[field.name] = df[field.name].astype(pd_type, copy=False)
#             else:
#                 df[field.name] = pd.Series(dtype=pd_type)
#         except Exception as e:
#             print(field.name)
#             print(df[field.name])
#             raise e


# TODO: any point to this? just adding missing columns as None
# def coerce_dictlist_to_otype(d: DictList, otype: ObjectType) -> DictList:
#     # sa_cols = ObjectTypeMapper(env).to_sqlalchemy(otype)
#     for field in otype.fields:
#         pd_type = field_type_to_pandas_type(field.field_type)
#         if field.name in df:
#             df[field.name] = df[field.name].astype(pd_type, copy=False)
#         else:
#             df[field.name] = pd.Series(dtype=pd_type)
#     return df


# def dataframe_to_sqlalchemy_schema():
#     # TODO
#     # SQLTable()._get_column_names_and_types(self._sqlalchemy_type)  see pandas.io.sql
#     pass
