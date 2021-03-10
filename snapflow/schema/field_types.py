import inspect
import decimal
from datetime import date, time, datetime, timedelta
from snapflow.utils.common import ensure_bool, ensure_date, ensure_datetime, ensure_time
from snapflow.storage.data_formats.records import Records
from snapflow.utils.registry import ClassBasedEnum, global_registry
from snapflow.utils.data import is_boolish, is_nullish, is_numberish, read_json
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type, Union

from dateutil import parser
import sqlalchemy.types as satypes


"""
- Numeric: bool, int(64), float(64), decimal
- String: bytes, longbytes, text, longtext
- Datetime: date, time, datetime (not doing for now: interval, epochtime)
- Misc: json (not doing for now: categorical/enum, xml)
"""

LONG_TEXT = 2 ** 16


class FieldTypeBase(ClassBasedEnum):
    sqlalchemy_type: Type[satypes.TypeEngine]
    python_type: type
    pandas_type: str
    parameter_names: List[str] = []
    defaults: List[Tuple[str, Any]] = []
    # Carindality rank represents how large a type's value set is.
    # IF a type can represent all of another type's values, it MUST have equal or higher cardinality
    # Otherwise, goes in order of actual cardinality (bit length)
    # 0 is smallest set (boolean) and then largest is 31 (large blob)
    # This allows us to choose the smallest compatible type, for instance
    # 0-9: small integer or less
    # 10-19: numerics, datetimes
    # 20-29: strings
    # 30-39: blobs
    cardinality_rank: int = 0
    max_bytes: int = 1
    # Types that can only represent a subset of this types values
    # subset_types: List[type] = []
    _kwargs: Dict[str, Any]

    def __init__(self, *args, **kwargs):
        sig = inspect.signature(self.sqlalchemy_type)
        self.parameter_names = list(sig.parameters.keys())
        _kwargs = dict(self.defaults)
        for i, arg in enumerate(args):
            name = self.parameter_names[i]
            _kwargs[name] = arg
        _kwargs.update(kwargs)
        self._kwargs = _kwargs

    def __repr__(self) -> str:
        s = self.name
        kwargs = ", ".join(
            [
                f"{n}={self._kwargs[n]}"
                for n in self.parameter_names
                if n in self._kwargs
            ]
        )
        if kwargs:
            s += f"({kwargs})"
        return s

    def __eq__(self, o: object) -> bool:
        return type(self) is type(o) and self._kwargs == o._kwargs

    def __hash__(self):
        return hash(repr(self))

    def as_sqlalchemy_type(self) -> satypes.TypeEngine:
        return self.sqlalchemy_type(**self._kwargs)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def to_json(self) -> str:
        return repr(self)

    def is_maybe(self, obj: Any) -> bool:
        return False

    def is_definitely(self, obj: Any) -> bool:
        return isinstance(obj, self.python_type)

    def cast(self, obj: Any, strict: bool = False, force: bool = False) -> Any:
        return obj


FieldType = FieldTypeBase
FieldTypeLike = Union[FieldTypeBase, str]


class CastException(Exception):
    to_field_type: FieldType
    obj: Any

    def __init__(self, to_field_type: FieldType, obj: Any):
        msg = f"Exception trying to cast {obj} to {to_field_type}"
        super().__init__(msg)
        self.to_field_type = to_field_type
        self.obj = obj


class CastWouldCauseDataLossException(CastException):
    pass


class UncastableException(CastException):
    pass


### Numeric types
class Boolean(FieldTypeBase):
    sqlalchemy_type = satypes.Boolean
    python_type = bool
    pandas_type = "boolean"
    cardinality_rank = 0
    max_bytes = 1

    def is_maybe(self, obj: Any) -> bool:
        return is_boolish(obj)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            return bool(obj)
        return ensure_bool(obj)


# Don't bother with smaller integer, it's 2020
# class Integer(FieldTypeBase):
#     sqlalchemy_type = satypes.Integer
#     python_type = int
#     cardinality_rank = 10
#     max_bytes = 4

#
#     def is_maybe(self, obj: Any) -> bool:
#         try:
#             i = int(obj)
#             return i < BIG_INT
#         except (ValueError, TypeError):
#             return False

#
#     def is_definitely(self, obj: Any) -> bool:
#         if isinstance(obj, bool):
#             return False
#         return isinstance(obj, int) and obj < BIG_INT


class Integer(FieldTypeBase):
    sqlalchemy_type = satypes.BigInteger
    python_type = int
    pandas_type = "Int64"
    cardinality_rank = 11
    max_bytes = 8

    def is_maybe(self, obj: Any) -> bool:
        try:
            i = int(obj)
            return True
        except (ValueError, TypeError):
            return False

    def is_definitely(self, obj: Any) -> bool:
        if isinstance(obj, bool):
            return False
        return isinstance(obj, int)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        return int(obj)


class Float(FieldTypeBase):
    sqlalchemy_type = satypes.Float
    python_type = float
    pandas_type = "float64"
    cardinality_rank = 12
    max_bytes = 8

    def is_maybe(self, obj: Any) -> bool:
        try:
            i = float(obj)
            return True
        except (ValueError, TypeError):
            return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        return float(obj)


class Decimal(FieldTypeBase):
    sqlalchemy_type = satypes.Numeric
    # defaults = {"precision": 16}
    python_type = decimal.Decimal
    pandas_type = "float64"
    cardinality_rank = 13
    max_bytes = 8

    def is_maybe(self, obj: Any) -> bool:
        try:
            i = float(obj)
            return True
        except (ValueError, TypeError):
            return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        return decimal.Decimal(obj)


### String types
class Text(FieldTypeBase):
    sqlalchemy_type = satypes.Unicode
    defaults = {"length": 255}
    python_type = str
    pandas_type = "string"
    cardinality_rank = 20
    max_bytes = LONG_TEXT

    def is_maybe(self, obj: Any) -> bool:
        return (isinstance(obj, str) or isinstance(obj, bytes)) and (
            len(obj) < LONG_TEXT
        )

    def is_definitely(self, obj: Any) -> bool:
        # Can't ever really be sure (TODO)
        return False
        # return isinstance(obj, str) and len(obj) < LONG_TEXT

    def cast(self, obj: Any, strict: bool = False) -> Any:
        s = str(obj)
        if len(s) >= LONG_TEXT:
            raise CastWouldCauseDataLossException(self, obj)
        return str(obj)


class LongText(FieldTypeBase):
    sqlalchemy_type = satypes.UnicodeText
    python_type = str
    pandas_type = "string"
    cardinality_rank = 21
    max_bytes = LONG_TEXT * LONG_TEXT

    def is_maybe(self, obj: Any) -> bool:
        return isinstance(obj, str) or isinstance(obj, bytes)

    def is_definitely(self, obj: Any) -> bool:
        # Can't ever really be sure (TODO)
        return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        return str(obj)


### Datetime types
class Date(FieldTypeBase):
    sqlalchemy_type = satypes.Date
    python_type = date
    pandas_type = "date"
    cardinality_rank = 10
    max_bytes = 4

    def is_maybe(self, obj: Any) -> bool:
        if isinstance(obj, date):
            return True
        if is_numberish(obj):
            # Numbers aren't dates!
            return False
        if not isinstance(obj, str):
            obj = str(obj)
        try:
            # We use ancient date as default to detect when no date was found
            # Will fail if trying to parse actual ancient dates!
            dt = parser.parse(obj, default=datetime(1, 1, 1))
            if dt.year < 2:
                # dateutil parser only found a time, not a date
                return False
        except Exception:
            return False
        return True

    def is_definitely(self, obj: Any) -> bool:
        if isinstance(obj, str) and 8 <= len(obj) <= 10:
            if is_numberish(obj):
                # Numbers aren't dates!
                return False
            try:
                parser.isoparse(obj)
                return True
            except:
                pass
            return False
        else:
            return isinstance(obj, date) and not isinstance(obj, datetime)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            if isinstance(obj, datetime):
                obj = obj.date()
            if not isinstance(obj, date):
                raise TypeError(obj)
            return obj
        return ensure_date(obj)


class DateTime(FieldTypeBase):
    sqlalchemy_type = satypes.DateTime
    python_type = datetime
    pandas_type = "datetime64[ns]"
    cardinality_rank = 12
    max_bytes = 8

    def is_maybe(self, obj: Any) -> bool:
        if isinstance(obj, datetime):
            return True
        if isinstance(obj, time):
            return False
        if is_numberish(obj):
            # Numbers aren't datetimes!
            return False
        if not isinstance(obj, str):
            obj = str(obj)
        try:
            dt = parser.parse(obj, default=datetime(1, 1, 1))
            if dt.year < 2:
                # dateutil parser only found a time, not a date
                return False
        except Exception:
            return False
        return True

    def is_definitely(self, obj: Any) -> bool:
        if isinstance(obj, str) and 14 <= len(obj) <= 26:
            if is_numberish(obj):
                # Numbers aren't dates!
                return False
            try:
                parser.isoparse(obj)
                return True
            except:
                pass
            return False
        else:
            return isinstance(obj, datetime)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            if isinstance(obj, date):
                obj = datetime(obj.year, obj.month, obj.day)
            if not isinstance(obj, datetime):
                raise TypeError(obj)
            return obj
        return ensure_datetime(obj)


class Time(FieldTypeBase):
    sqlalchemy_type = satypes.Time
    python_type = time
    pandas_type = "time"
    cardinality_rank = 12
    max_bytes = 8

    def is_maybe(self, obj: Any) -> bool:
        if isinstance(obj, time):
            return True
        if is_numberish(obj):
            # Numbers aren't times!
            return False
        if not isinstance(obj, str):
            obj = str(obj)
        try:
            # We use ancient date as default to detect when only time was found
            # Will fail if trying to parse actual ancient dates!
            dt = parser.parse(obj, default=datetime(1, 1, 1))
            if dt.year < 2:
                # dateutil parser found just a time
                return True
        except Exception:
            return False
        return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            if not isinstance(obj, time):
                raise TypeError(obj)
            return obj
        return ensure_time(obj)


# TODO: Not well supported? Just use int?
# class Interval(FieldTypeBase):
#     sqlalchemy_type = satypes.Interval
#     python_type = timedelta
#     cardinality_rank = 12
#     max_bytes = 8

# TODO: Not well supported? Just use int / float?
# class EpochTime(FieldTypeBase):
#   pass


### Misc types
class JSON(FieldTypeBase):
    sqlalchemy_type = satypes.JSON
    python_type = dict
    pandas_type = "object"
    cardinality_rank = 0  # TODO: strict json, only dicts and lists?
    max_bytes = LONG_TEXT * LONG_TEXT

    def is_maybe(self, obj: Any) -> bool:
        # TODO: strings too? (Actual json string)
        return isinstance(obj, dict) or isinstance(obj, list)

    def is_definitely(self, obj: Any) -> bool:
        return isinstance(obj, dict) or isinstance(obj, list)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            if not isinstance(obj, dict) and not isinstance(obj, list):
                raise TypeError(obj)
            return obj
        if isinstance(obj, dict) or isinstance(obj, list):
            return obj
        if isinstance(obj, str):
            return read_json(obj)
        return [obj]  # TODO: this is extra ugly, should we just fail?


all_types = [
    Boolean,
    Integer,
    Float,
    Decimal,
    Date,
    Time,
    DateTime,
    Text,
    LongText,
    JSON,
]
all_types_instantiated = [ft() for ft in all_types]
for ft in all_types:
    global_registry.register(ft)

DEFAULT_FIELD_TYPE = Text()


def ensure_field_type(
    ft: Union[str, FieldType, Type[FieldType], satypes.TypeEngine]
) -> FieldType:
    # TODO: this is hidden and only affects this code path... Where do we want to conform this?
    if ft is None:
        return DEFAULT_FIELD_TYPE
    if isinstance(ft, FieldType):
        return ft
    if isinstance(ft, type) and issubclass(ft, FieldType):
        return ft()
    if isinstance(ft, satypes.TypeEngine):
        # Cast sqlalchemy type to its string representation for eval below ("Unicode(55)")
        ft = repr(ft)
    if isinstance(ft, str):
        return str_to_field_type(ft)
    raise NotImplementedError(ft)


def str_to_field_type(s: str) -> FieldType:
    local_vars = {f().name: f for f in all_types}
    satype_aliases = {
        "INTEGER": Integer,
        "BigInteger": Integer,
        "Numeric": Decimal,
        "NUMERIC": Decimal,
        "REAL": Float,
        "DATE": Date,
        "TEXT": Text,
        "VARCHAR": Text,
        "Unicode": Text,
        "UnicodeText": Text,
        "Json": JSON,
    }
    local_vars.update(satype_aliases)
    try:
        ft = eval(s, {"__builtins__": None}, local_vars)
        if isinstance(ft, type):
            ft = ft()
        return ft
    except AttributeError:
        raise NotImplementedError(s)

