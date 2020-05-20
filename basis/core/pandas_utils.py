import pandas as pd
from pandas import DataFrame

from basis.core.object_type import ObjectType


def coerce_dataframe_to_otype(df: DataFrame, otype: ObjectType):
    # sa_cols = ObjectTypeMapper(env).to_sqlalchemy(otype)
    for field in otype.fields:
        pd_type = field_type_to_pandas_type(field.field_type)
        try:
            if field.name in df:
                if df[field.name].dtype.name == pd_type:
                    continue
                df[field.name] = df[field.name].astype(pd_type, copy=False)
            else:
                df[field.name] = pd.Series(dtype=pd_type)
        except Exception as e:
            print(field.name)
            print(df[field.name])
            raise e


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


def dataframe_to_sql_schema():
    # TODO
    # SQLTable()._get_column_names_and_types(self._sqlalchemy_type)  see pandas.io.sql
    pass


def field_type_to_pandas_type(ft: str) -> str:
    ft = ft.lower()
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
