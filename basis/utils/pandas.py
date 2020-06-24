from __future__ import annotations

from typing import List

import pandas as pd
from basis.core.data_formats import RecordsList
from basis.core.typing.object_type import ObjectType
from basis.utils.data import is_nullish, records_list_as_dict_of_lists
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
    df1: DataFrame, df2: DataFrame, otype: ObjectType, ignored_columns: List[str] = None
):
    if ignored_columns:
        df1 = df1[[c for c in df1.columns if c not in ignored_columns]]
        df2 = df2[[c for c in df2.columns if c not in ignored_columns]]
    assert df1.shape == df2.shape, f"Different shapes: {df1.shape} {df2.shape}"
    assert set(df1.columns) == set(df2.columns)
    # TODO: for now giving benefit of doubt and try to cast dtypes, need to rethink as part of type improvements tho
    # try: df1 = df1.astype(df2.dtypes)
    # except: pass
    # for c, d, dd in zip(df1.columns, list(df1.dtypes), list(df2.dtypes)):
    #     print(f"{c:30} {str(d):20}", str(dd))
    df1.sort_values(otype.unique_on, inplace=True)
    df2.sort_values(otype.unique_on, inplace=True)
    for (i, r), (i2, r2) in zip(df1.iterrows(), df2.iterrows()):
        for c in r.keys():
            assert (is_nullish(r[c]) and is_nullish(r2[c])) or r[c] == r2[c]


def empty_dataframe_for_otype(otype: ObjectType) -> DataFrame:
    from basis.core.typing.inference import sqlalchemy_type_to_pandas_type

    df = DataFrame()
    for field in otype.fields:
        pd_type = sqlalchemy_type_to_pandas_type(field.field_type)
        df[field.name] = Series(dtype=pd_type)
    return df


def records_list_to_dataframe(records: RecordsList, otype: ObjectType) -> DataFrame:
    from basis.core.typing.inference import conform_dataframe_to_otype

    df = DataFrame(records)
    return conform_dataframe_to_otype(df, otype)
    # series = records_list_as_dict_of_lists(records)
    # df = DataFrame()
    # # print("=========")
    # # print(otype.fields)
    # for n, s in series.items():
    #     f = otype.get_field(n)
    #     if f is None:
    #         dtype = None
    #     else:
    #         dtype = sqlalchemy_type_to_pandas_type(f.field_type)
    #     # print(n, dtype, s)
    #     try:
    #         df[n] = Series(s, dtype=dtype)
    #     except (TypeError, ValueError) as e:
    #         # print("Type error:", n, dtype, s)
    #         df[n] = Series(s)
    #         df[n] = df[n].infer_objects()
    # # print("Made DF: ", df.dtypes, id(df))
    # return df


def dataframe_to_records_list(df: DataFrame, otype: ObjectType = None) -> RecordsList:
    for c in df:
        dfc = df[c].astype(object)
        dfc.loc[pd.isna(dfc)] = None
        df[c] = dfc
    return df.to_dict(orient="records")

    # astype("M8[us]").astype(object)
