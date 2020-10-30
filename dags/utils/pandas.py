from __future__ import annotations

from typing import List, Optional

import pandas as pd
from dags.core.data_formats import RecordsList
from dags.core.typing.object_schema import ObjectSchema
from dags.utils.data import is_nullish, records_list_as_dict_of_lists
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
    df1: DataFrame,
    df2: DataFrame,
    schema: Optional[ObjectSchema] = None,
    ignored_columns: List[str] = None,
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
    if schema is not None:
        df1.sort_values(schema.unique_on, inplace=True)
        df2.sort_values(schema.unique_on, inplace=True)
    for (i, r), (i2, r2) in zip(df1.iterrows(), df2.iterrows()):
        for c in r.keys():
            if is_nullish(r[c]) and is_nullish(r2[c]):
                continue
            assert_almost_equal(r[c], r2[c])


def empty_dataframe_for_schema(schema: ObjectSchema) -> DataFrame:
    from dags.core.typing.inference import sqlalchemy_type_to_pandas_type

    df = DataFrame()
    for field in schema.fields:
        pd_type = sqlalchemy_type_to_pandas_type(field.field_type)
        df[field.name] = Series(dtype=pd_type)
    return df


def records_list_to_dataframe(records: RecordsList, schema: ObjectSchema) -> DataFrame:
    from dags.core.typing.inference import conform_dataframe_to_schema

    df = DataFrame(records)
    return conform_dataframe_to_schema(df, schema)
    # series = records_list_as_dict_of_lists(records)
    # df = DataFrame()
    # # print("=========")
    # # print(schema.fields)
    # for n, s in series.items():
    #     f = schema.get_field(n)
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


def dataframe_to_records_list(
    df: DataFrame, schema: ObjectSchema = None
) -> RecordsList:
    for c in df:
        dfc = df[c].astype(object)
        dfc.loc[pd.isna(dfc)] = None
        df[c] = dfc
    return df.to_dict(orient="records")

    # astype("M8[us]").astype(object)
