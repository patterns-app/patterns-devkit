from __future__ import annotations

from datetime import datetime

from basis.core.declarative.base import FrozenPydanticBase
from sqlalchemy.sql.sqltypes import Enum

# TODO: lots of useful events we could publish from process


class EventVerbs(str, Enum):
    # Execution
    execution_start = "execution_start"
    execution_end = "execution_end"
    execution_error = "execution_error"
    # Result
    execution_result = "execution_result"


class Event(FrozenPydanticBase):
    uid: str
    occurred: datetime = Field(default_factory=utcnow)
    verb: str
    subject_uid: str
    direct_object_uid: str
    secondary_object_uid: str
    value: float
    message: str
    context: dict
