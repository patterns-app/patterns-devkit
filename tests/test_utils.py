from __future__ import annotations

import json
import tempfile
from datetime import date, datetime, time, timedelta

import pytest
from numpy import NaN
from pandas import DataFrame

from dags.utils.common import (
    DagsJSONEncoder,
    StringEnum,
    is_datetime_str,
    snake_to_title_case,
    title_to_snake_case,
    rand_str,
)
from dags.utils.data import is_nullish
from dags.utils.pandas import (
    assert_dataframes_are_almost_equal,
    dataframe_to_records_list,
    empty_dataframe_for_otype,
)
from tests.utils import TestType4


def test_snake_and_title_cases():
    assert snake_to_title_case("_hello_world") == "HelloWorld"
    assert snake_to_title_case("_hello__world") == "HelloWorld"
    assert snake_to_title_case("_hello__world_") == "HelloWorld"
    assert snake_to_title_case("_hello_world_goodbye") == "HelloWorldGoodbye"
    assert snake_to_title_case("hello") == "Hello"
    assert snake_to_title_case("") == ""
    # t -> s
    assert title_to_snake_case("HelloWorld") == "hello_world"
    assert title_to_snake_case("Hello") == "hello"
    assert title_to_snake_case("hello") == "hello"
    assert title_to_snake_case("HELLO") == "hello"
    assert title_to_snake_case("helloWorld") == "hello_world"
    assert title_to_snake_case("helloWorldGoodbye") == "hello_world_goodbye"


def test_is_datetime_str():
    assert is_datetime_str("2012/01/01")
    assert is_datetime_str("1/1/2012")
    assert is_datetime_str("2012-01-01")
    assert is_datetime_str("2012-01-01T00:00:00")
    assert is_datetime_str("2012-01-01T00:00:00Z")
    assert is_datetime_str("2012-01-01 00:00:00+08")
    assert is_datetime_str("2012-01-01 00:00:00.001+08")
    assert is_datetime_str("January 2012")  # TODO: False positive?
    # Not dt strs (too short or not date like)
    assert not is_datetime_str("2012")
    assert not is_datetime_str("3/2012")  # TODO: False negative?
    assert not is_datetime_str("Pizza 2012-02-02")


def test_json_encoder():
    class T(StringEnum):
        A = "A"

    d = dict(
        dt=datetime(2012, 1, 1),
        d=date(2012, 1, 1),
        t=time(12, 1, 1),
        td=timedelta(days=1),
        o={1: 2, 3: 4},
        s="hello",
        f=1 / 9,
        e=T.A,
    )
    s = json.dumps(d, cls=DagsJSONEncoder)
    print(s)
    assert (
        s.strip()
        == """
    {"dt": "2012-01-01T00:00:00", "d": "2012-01-01", "t": "12:01:01", "td": "P1DT00H00M00S", "o": {"1": 2, "3": 4}, "s": "hello", "f": 0.1111111111111111, "e": "A"}
    """.strip()
    )


def test_assert_dataframes_are_almost_equal():
    df1 = DataFrame({"f1": range(10), "f2": range(10)})
    df2 = DataFrame({"f1": range(10), "f2": range(10)})
    df3 = DataFrame({"f1": range(10), "f2": range(10), "c": range(10)})
    df4 = DataFrame({"f1": range(20), "f2": range(20)})
    assert_dataframes_are_almost_equal(df1, df2, TestType4)
    with pytest.raises(AssertionError):
        assert_dataframes_are_almost_equal(df1, df3, TestType4)
    with pytest.raises(AssertionError):
        assert_dataframes_are_almost_equal(df1, df4, TestType4)


def test_is_emptyish():
    assert is_nullish(None)
    assert is_nullish("NULL")
    assert is_nullish("null")
    assert is_nullish("NA")
    assert is_nullish("")
    assert is_nullish(NaN)
    assert not is_nullish(0)
    assert not is_nullish(".")
    assert not is_nullish("0")


def test_empty_dataframe_from_otype():
    df = empty_dataframe_for_otype(TestType4)
    assert set(df.columns) == {"f1", "f2"}
    df = empty_dataframe_for_otype(TestType4)
    assert set(d.name for d in df.dtypes) == {"string", "int32"}


def test_dataframe_to_records_list():
    df = DataFrame({"a": range(10), "b": range(10)})
    assert dataframe_to_records_list(df) == [{"a": i, "b": i} for i in range(10)]
    df["c"] = datetime(2012, 1, 1)
    df.loc[0, "c"] = None  # Add a NaT
    records = dataframe_to_records_list(df)
    for r in records:
        if r["a"] == 0:
            # NaT has been converted to None
            assert r["c"] is None


def get_tmp_sqlite_db_url(dbname=None):
    if dbname is None:
        dbname = rand_str(10)
    dir = tempfile.mkdtemp()
    return f"sqlite:///{dir}/{dbname}.db"


# def test_coerce_dataframe_to_otype():
#     df = DataFrame({"f1": range(10), "f2": range(10)})
#     df = coerce_dataframe_to_otype(df, TestType4)
#     dfe = DataFrame({"f1": [str(i) for i in range(10)], "f2": range(10)})
#     assert_dataframes_are_almost_equal(df, dfe, TestType4)
