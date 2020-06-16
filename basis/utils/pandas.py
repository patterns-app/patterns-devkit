from __future__ import annotations

from typing import List

import pandas as pd
from basis.core.data_format import RecordsList
from basis.core.typing.object_type import ObjectType
from basis.utils.data import records_list_as_dict_of_lists
from pandas import DataFrame, Index, Series
from pandas._testing import assert_almost_equal


def sortable_columns(dtypes: Series) -> List[str]:
    sortables = []
    for i in dtypes.index:
        c = str(dtypes[i]).lower()
        if "cat" in c or "obj" in c:
            continue
        sortables.append(i)
    return sortables


def assert_dataframes_are_almost_equal(
    df1: DataFrame, df2: DataFrame, otype: ObjectType
):
    assert df1.shape == df2.shape, f"{df1.shape} {df2.shape}"
    assert set(df1.columns) == set(df2.columns)
    df1.sort_values(otype.unique_on, inplace=True)
    df2.sort_values(otype.unique_on, inplace=True)
    assert_almost_equal(df1, df2, check_dtype=False)


def empty_dataframe_for_otype(otype: ObjectType) -> DataFrame:
    from basis.core.typing.inference import sqlalchemy_type_to_pandas_type

    df = DataFrame()
    for field in otype.fields:
        pd_type = sqlalchemy_type_to_pandas_type(field.field_type)
        df[field.name] = Series(dtype=pd_type)
    return df


def records_list_to_dataframe(records: RecordsList, otype: ObjectType) -> DataFrame:
    from basis.core.typing.inference import sqlalchemy_type_to_pandas_type

    series = records_list_as_dict_of_lists(records)
    df = DataFrame()
    # print("=========")
    # print(otype.fields)
    for n, s in series.items():
        f = otype.get_field(n)
        if f is None:
            dtype = None
        else:
            dtype = sqlalchemy_type_to_pandas_type(f.field_type)
        # print(n, dtype, s)
        try:
            df[n] = Series(s, dtype=dtype)
        except (TypeError, ValueError) as e:
            # print("Type error:", n, dtype, s)
            df[n] = Series(s)
            df[n] = df[n].infer_objects()
    # print("Made DF: ", df.dtypes, id(df))
    return df


def dataframe_to_records_list(df: DataFrame, otype: ObjectType = None) -> RecordsList:
    for c in df:
        dfc = df[c].astype(object)
        dfc.loc[pd.isna(dfc)] = None
        df[c] = dfc
    return df.to_dict(orient="records")

    # astype("M8[us]").astype(object)


# def coerce_dataframe_to_otype(df: DataFrame, otype: ObjectType):
#     from basis.core.typing.inference import sqlalchemy_type_to_pandas_type
#     for field in otype.fields:
#         pd_type = sqlalchemy_type_to_pandas_type(field.field_type)
#         try:
#             if field.name in df:
#                 if df[field.name].dtype.name == pd_type:
#                     continue
#                 # TODO: `astype` is not aggressive enough (won't cast values), so doesn't work
#                 # Likely need combo of "hard" conversion using `to_*` methods and `infer_objects`
#                 # and explicit individual python casts if that fails
#                 df[field.name] = df[field.name].astype(pd_type, copy=False)
#             else:
#                 df[field.name] = Series(dtype=pd_type)
#         except Exception as e:
#             print(field.name)
#             print(df[field.name])
#             raise e
