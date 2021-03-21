import decimal
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, List, Type

import pandas as pd
import pytest
from dateutil.tz import tzoffset
from snapflow.schema.casting import cast_python_object_to_field_type
from snapflow.schema.field_types import (
    DEFAULT_FIELD_TYPE,
    LONG_TEXT,
    Boolean,
    Date,
    DateTime,
    Decimal,
    FieldType,
    Float,
    Integer,
    Json,
    LongText,
    Text,
    Time,
    all_types,
    ensure_field_type,
)
from snapflow.schema.inference import pandas_series_to_field_type, select_field_type

nullish = [None, "None", "null", "none"]
bool_ = True
int_ = 2 ** 16
big_int = 2 ** 33
float_ = 1.119851925872322
floatstr = "1.119851925872322"
decimal_ = decimal.Decimal("109342342.123")
date_ = date(2020, 1, 1)
datestr = "1/1/2020"
dateisostr = "2020-01-01"
datetime_ = datetime(2020, 1, 1)
datetimestr = "2017-02-17T15:09:26-08:00"
timestr = "15:09:26"
time_ = time(20, 1, 1)
long_text = "helloworld" * int(LONG_TEXT / 9)
json_ = {"hello": "world"}


@dataclass
class Case:
    obj: Any
    maybes: List[Type[FieldType]]
    definitelys: List[Type[FieldType]]


numeric_types: List[FieldType] = [Integer, Float, Decimal]
string_types: List[FieldType] = [Text, LongText]

cases = [
    Case(
        obj=bool_,
        maybes=[Boolean] + numeric_types,
        definitelys=[Boolean],
    ),
    Case(
        obj=int_,
        maybes=numeric_types,
        definitelys=[Integer],
    ),
    Case(
        obj=big_int,
        maybes=[Integer, Float, Decimal],
        definitelys=[Integer],
    ),
    Case(
        obj=float_,
        maybes=[Float, Decimal, Integer, Integer],
        definitelys=[Float],
    ),
    Case(
        obj=floatstr,
        maybes=string_types + [Float, Decimal],
        definitelys=[],
    ),
    Case(
        obj=date_,
        maybes=[DateTime, Date],
        definitelys=[Date],
    ),
    Case(
        obj=datestr,
        maybes=string_types + [DateTime, Date],
        definitelys=[],
    ),
    Case(
        obj=dateisostr,
        maybes=string_types + [DateTime, Date],
        definitelys=[Date],
    ),
    Case(
        obj=datetime_,
        maybes=[DateTime, Date],
        definitelys=[DateTime],
    ),
    Case(
        obj=datetimestr,
        maybes=string_types + [DateTime, Date],
        definitelys=[DateTime],
    ),
    Case(
        obj=time_,
        maybes=[Time],
        definitelys=[Time],
    ),
    Case(
        obj=timestr,
        maybes=string_types + [Time],
        definitelys=[],
    ),
    Case(obj=long_text, maybes=[LongText], definitelys=[]),
    Case(obj=json_, maybes=[Json], definitelys=[Json]),
]


@pytest.mark.parametrize("case", cases)
def test_case(case: Case):
    maybes = []
    defs = []
    for ft in all_types:
        if isinstance(ft, type):
            ft = ft()
        if ft.is_maybe(case.obj):
            maybes.append(type(ft))
        if ft.is_definitely(case.obj):
            defs.append(type(ft))
    assert set(maybes) == set(case.maybes)
    assert set(defs) == set(case.definitelys)


sample_records = [
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "1/1/2020",
        "c": "2020",
        "d": [1, 2, 3],
        "e": {1: 2},
        "f": "1.3",
        "g": 123,
        "h": "null",
        "i": None,
    },
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "1/1/2020",
        "c": "12",
        "d": [1, 2, 3],
        "e": {1: 2},
        "f": "cookies",
        "g": 123,
        "h": "null",
        "i": None,
    },
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "30/30/2020",
        "c": "12345",
        "d": [1, 2, 3],
        "e": "string",
        "f": "true",
        "g": 12345,
        "h": long_text,
    },
    {
        "a": None,
        "b": None,
        "c": None,
        "d": None,
        "e": None,
        "f": None,
        "g": None,
        "i": None,
    },
]

expected_field_types = {
    "a": DateTime(),
    "b": Text(),
    "c": Integer(),
    "d": Json(),
    "e": Text(),
    "f": Text(),
    "g": Integer(),
    "h": LongText(),
    "i": DEFAULT_FIELD_TYPE,
}
expected_pandas_field_types = expected_field_types.copy()
expected_pandas_field_types[
    "g"
] = Float()  # Pandas doesn't handle nullable ints by default
expected_pandas_field_types["i"] = Float()  # Pandas defaults to float when all NA


def test_select_field_type():
    for k in sample_records[0].keys():
        values = [r.get(k) for r in sample_records]
        ft = select_field_type(values)
        assert ft == expected_field_types[k], k


ERROR = "_ERROR"

D = decimal.Decimal


@pytest.mark.parametrize(
    "ftype,obj,expected",
    [
        (Boolean, "true", True),
        (Boolean, "false", False),
        (Boolean, "t", True),
        (Boolean, "f", False),
        (Boolean, 1, True),
        (Boolean, 0, False),
        (Boolean, "Hi", ERROR),
        (Integer, 1, 1),
        (Integer, "1", 1),
        (Integer, "01", 1),
        (Integer, " 1", 1),
        (Integer, " 1.1", ERROR),
        (Integer, "2020-01-01", ERROR),
        (Integer, pd.NA, None),
        (Integer, None, None),
        (Float, 1, 1.0),
        (Float, "1", 1.0),
        (Float, "01", 1.0),
        (Float, " 1", 1.0),
        (Float, " 1.1", 1.1),
        (Float, " 2342311341.100", 2342311341.100),
        (Float, "2020-01-01", ERROR),
        (Float, pd.NA, None),
        (Float, None, None),
        (Decimal, D(1.01), D(1.01)),
        (Decimal, "1", D("1.0")),
        (Decimal, "01", D("1.0")),
        (Decimal, " 1", D("1.0")),
        (Decimal, " 1.1", D("1.1")),
        (Decimal, " 2342311341.100", D("2342311341.100")),
        (Decimal, "2020-01-01", ERROR),
        (Decimal, pd.NA, None),
        (Decimal, None, None),
        # TODO: rest
        (Date, date(2020, 1, 1), date(2020, 1, 1)),
        (Date, "2020-01-01", date(2020, 1, 1)),
        (Date, "2020-01-01 00:00:00", date(2020, 1, 1)),
        (Date, 1577836800, ERROR),
        (Date, "Hello world", ERROR),
        (Time, time(1, 0, 1), time(1, 0, 1)),
        (Time, "01:00:01", time(1, 0, 1)),
        (Time, "01:00", time(1, 0, 0)),
        (Time, "hello world", ERROR),
        (DateTime, "2020-01-01", datetime(2020, 1, 1)),
        (DateTime, "2020-01-01 00:00:00", datetime(2020, 1, 1)),
        (
            DateTime,
            "2020-01-01T00:00:00-08:00",
            datetime(
                2020, 1, 1, tzinfo=tzoffset(None, -28800)
            ),  # TODO: this offset is what we want?
        ),
        (DateTime, 1577836800, datetime(2020, 1, 1)),
        (DateTime, "Hello world", ERROR),
        (Json, [1, 2], [1, 2]),
        (Json, {"a": 2}, {"a": 2}),
        (Json, '{"1":2}', {"1": 2}),
        # (Json, None, None),
        # (Boolean, "Hi", None),  # If we ignore the error, it should be null
        (Date, "2020-01-01", date(2020, 1, 1)),
        (Date, "2020-01-01 00:00:00", date(2020, 1, 1)),
        (Date, "Hi", ERROR),
        (Text, None, None),
        (Text, "Hi", "Hi"),
        (Text, 0, "0"),
        # TODO: when should these be none?
        (Text, "null", "null"),
        (Text, "None", "None"),
        (Text, "NA", "NA"),
        (Text, long_text, ERROR),
        (LongText, None, None),
        (LongText, long_text, long_text),
        (LongText, "Hi", "Hi"),
        (LongText, 0, "0"),
    ],
    ids=lambda x: str(x)[:30],
)
def test_value_soft_casting(ftype, obj, expected):
    if isinstance(ftype, type):
        ftype = ftype()
    if expected == ERROR:
        with pytest.raises(Exception):
            cast_python_object_to_field_type(obj, ftype)
    else:
        assert cast_python_object_to_field_type(obj, ftype) == expected


def test_pandas_series_to_field_type():
    df = pd.DataFrame.from_records(sample_records)
    for c in df.columns:
        assert pandas_series_to_field_type(df[c]) == expected_pandas_field_types[c], c


@pytest.mark.parametrize("ftype,expected", [("Text(length=55)", Text(length=55))])
def test_ensure_field_type(ftype, expected):
    assert ensure_field_type(ftype) == expected
