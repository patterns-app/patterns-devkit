from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Union

from snapflow.utils.common import StringEnum, utcnow


class EventSubject(StringEnum):
    EXECUTION = "execution"
    DATABLOCK = "datablock"
    EXTERNAL_RESOURCE = "external_resource"


def ensure_event_subject(e) -> EventSubject:
    if isinstance(e, str):
        return EventSubject(e)
    return e


class EventLevel(StringEnum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"


def ensure_event_level(e) -> EventLevel:
    if isinstance(e, str):
        return EventLevel(e)
    return e


@dataclass(frozen=True)
class Event:
    event_subject: EventSubject
    event_details: Dict[str, Any]
    level: EventLevel
    occurred_at: datetime
    node_key: Optional[str] = None
    pipe_name: Optional[str] = None
    external_resource_name: Optional[str] = None
    environment_name: Optional[str] = None


def event_factory(
    event_subject: Union[EventSubject, str], event_details, **kwargs
) -> Event:
    return Event(
        event_subject=ensure_event_subject(event_subject),
        event_details=event_details,
        occurred_at=utcnow(),
        **kwargs,
    )


class EventHandler:
    def handle(self, e: Event):
        raise NotImplementedError


class ConsoleHandler(EventHandler):
    def handle(self, e: Event):
        print(self.format_event(e))

    def format_event(self, e: Event) -> str:
        return f"{e.occurred_at.strftime('%F %T')} {e.event_subject} {e.event_details}"
