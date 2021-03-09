from datetime import date, time, datetime, timedelta
from decimal import Decimal
from snapflow.utils.data import is_boolish
from typing import Any, Dict, Type

import sqlalchemy.types as satypes


"""
- Numeric: (smallint), int, bigint, float, decimal
- String: bytes, longbytes, text, longtext
- Datetime: date, time, datetime, epochtime, interval
- Misc: boolean, json, enum/categorical
"""

BIG_INT = 2147483648


class FieldTypeMeta(type):
    sqlalchemy_type: Type[satypes.TypeEngine]
    python_type: type
    defaults: Dict[str, Any] = {}

    @property
    def name(self: Type) -> str:
        return self.__name__

    def __repr__(self) -> str:
        s = self.name
        args = ", ".join([f"{k}={v}" for k, v in self.defaults.items()])
        if args:
            s += f"({args})"
        return s


class FieldTypeBase(metaclass=FieldTypeMeta):
    @classmethod
    def is_maybe(cls, obj: Any) -> bool:
        return True

    @classmethod
    def is_definitely(cls, obj: Any) -> bool:
        return isinstance(cls.python_type, obj)


### Numeric types
class Boolean(FieldTypeBase):
    sqlalchemy_type = satypes.Boolean
    python_type: bool

    @classmethod
    def is_maybe(cls, obj: Any) -> bool:
        return is_boolish(obj)


class Integer(FieldTypeBase):
    sqlalchemy_type = satypes.Integer
    python_type: int

    @classmethod
    def is_maybe(cls, obj: Any) -> bool:
        try:
            i = int(obj)
            return i < BIG_INT
        except (ValueError, TypeError):
            return False

    @classmethod
    def is_definitely(cls, obj: Any) -> bool:
        return isinstance(obj, int) and obj < BIG_INT


class BigInteger(FieldTypeBase):
    sqlalchemy_type = satypes.BigInteger
    python_type: int

    @classmethod
    def is_maybe(cls, obj: Any) -> bool:
        try:
            i = int(obj)
            return True
        except (ValueError, TypeError):
            return False

    @classmethod
    def is_definitely(cls, obj: Any) -> bool:
        return isinstance(obj, int) and obj >= BIG_INT


class Float(FieldTypeBase):
    sqlalchemy_type = satypes.Float
    python_type: float


class Decimal(FieldTypeBase):
    sqlalchemy_type = satypes.Numeric
    # defaults = {"precision": 16}
    python_type: Decimal


### String types
class Text(FieldTypeBase):
    sqlalchemy_type = satypes.Unicode
    defaults = {"length": 255}
    python_type: str


class LongText(FieldTypeBase):
    sqlalchemy_type = satypes.UnicodeText
    python_type: str


### Datetime types
class Date(FieldTypeBase):
    sqlalchemy_type = satypes.Date
    python_type: date


class DateTime(FieldTypeBase):
    sqlalchemy_type = satypes.DateTime
    python_type: datetime


class Time(FieldTypeBase):
    sqlalchemy_type = satypes.Time
    python_type: time


class Interval(FieldTypeBase):
    sqlalchemy_type = satypes.Interval
    python_type: timedelta


### Misc types
class JSON(FieldTypeBase):
    sqlalchemy_type = satypes.JSON
    python_type: dict

