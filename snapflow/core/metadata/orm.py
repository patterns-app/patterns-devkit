from collections import OrderedDict
from typing import Any

from commonmodel import Schema
from dcp.data_format.base import DataFormat, DataFormatBase, get_format_for_nickname
from dcp.utils.common import rand_str, title_to_snake_case, utcnow
from snapflow.utils.output import cf
from sqlalchemy import Column, DateTime, Integer, String, func, types
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm.exc import DetachedInstanceError

SNAPFLOW_METADATA_TABLE_PREFIX = "_snapflow_"


class _BaseModel:
    @declared_attr
    def __tablename__(cls):
        return SNAPFLOW_METADATA_TABLE_PREFIX + title_to_snake_case(cls.__name__)  # type: ignore

    env_id = Column(String(length=64), default="default")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return self._repr()

    def _repr(self, **fields: Any) -> str:
        """
        Helper for __repr__
        From: https://stackoverflow.com/questions/55713664/sqlalchemy-best-way-to-define-repr-for-large-tables
        """
        field_strings = []
        at_least_one_attached_attribute = False
        for key, field in fields.items():
            try:

                if isinstance(field, Schema):
                    field_strings.append(f"{key}={field.name}")
                elif key in ("id", "url"):
                    field_strings.append(str(cf.bold_italic(f"{key}={field!r}")))
                else:
                    field_strings.append(f"{key}={field!r}")
            except DetachedInstanceError:
                field_strings.append(f"{key}=DetachedInstanceError")
            else:
                at_least_one_attached_attribute = True
        if at_least_one_attached_attribute:
            # if len(field_strings) > 1:
            fields_string = ", ".join(field_strings)
            return f"<{self.__class__.__name__}({fields_string})>"
        return f"<{self.__class__.__name__} {id(self)}>"

    def _asdict(self):
        result = OrderedDict()
        for key in self.__mapper__.c.keys():  # type: ignore
            result[key] = getattr(self, key)
        return result


BaseModel = declarative_base(cls=_BaseModel)  # type: Any


last_second: str = ""


def timestamp_increment_key(prefix: str = "", max_length: int = 28) -> str:
    """
    Generates keys that are unique and monotonic in time for a given run.
    Appends random chars to ensure multiple processes can run at once and not collide.
    """
    curr_ms = utcnow().strftime("%y%m%d_%H%M%S%f")
    rand_len = max_length - (21 + len(prefix))
    key = f"{prefix}_{curr_ms}_{rand_str(rand_len).lower()}"
    return key


class DataFormatType(types.TypeDecorator):
    impl = types.Unicode

    def __init__(self, length: int = 128):
        super().__init__(length)

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if not issubclass(value, DataFormatBase):
            raise TypeError(value)
        return value.nickname

    def process_result_value(self, value, dialect) -> DataFormat:
        if value is None:
            return None
        return get_format_for_nickname(value)


### Custom type ideas. Not needed atm

# class JsonEncodedDict(TypeDecorator):
#     """Enables JSON storage by encoding and decoding on the fly."""
#
#     # json_type = MutableDict.as_mutable(JSONEncodedDict) For if you want mutability
#
#     impl = types.Unicode
#
#     def process_bind_param(self, value, dialect):
#         if value is not None:
#             value = json.dumps(value)
#         return value
#
#     def process_result_value(self, value, dialect):
#         if value is not None:
#             value = json.loads(value)
#         return value


# class Schema(TypeDecorator):
#     impl = types.Unicode
#
#     def process_bind_param(self, value: Union[Schema, str], dialect) -> str:
#         if value is None:
#             return None
#         if isinstance(value, str):
#             module, name = value.split(".")
#         else:
#             module = value.module
#             name = value.name
#         return f"{module}.{name}"
#
#     def process_result_value(self, value: str, dialect) -> str:
#         if value is None:
#             return None
#         return value
