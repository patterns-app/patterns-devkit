from __future__ import annotations

import decimal
import hashlib
import json
import random
import re
import string
import uuid
from dataclasses import dataclass, fields
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import pytz
from colorful import Colorful
from dateutil import parser
from halo import Halo

DEBUG = False


def printd(*o):
    if DEBUG:
        print(*[cf.dimmed(i) for i in o])


class StringEnum(Enum):
    def __str__(self):
        return self.value


def title_to_snake_case(s: str) -> str:
    return re.sub(r"([a-z])([A-Z])", r"\1_\2", s).lower()


def snake_to_title_case(s: str) -> str:
    s2 = ""
    title = True
    for c in s:
        if c == "_":
            title = True
            continue
        if title:
            c = c.upper()
            title = False
        s2 += c
    return s2


UNAMBIGUOUS_ALPHA = "abcdefghjkmnpqrstuvwxyz"
UNAMBIGUOUS_CHARACTERS = UNAMBIGUOUS_ALPHA + UNAMBIGUOUS_ALPHA.upper() + string.digits


def rand_str(chars=20, character_set=UNAMBIGUOUS_CHARACTERS):
    rand = random.SystemRandom()
    return "".join(rand.choice(character_set) for _ in range(chars))


def utcnow():
    return pytz.utc.localize(datetime.utcnow())


def md5_hash(s: str) -> str:
    h = hashlib.md5()
    h.update(s.encode("utf8"))
    return h.hexdigest()


def dataclass_kwargs(dc: Union[dataclass, Type[dataclass]], kwargs: Dict) -> Dict:
    return {f.name: kwargs.get(f.name) for f in fields(dc)}


def ensure_list(x: Any) -> List:
    if x is None:
        return []
    if isinstance(x, List):
        return x
    return [x]


def ensure_datetime(x: Optional[Union[str, datetime]]) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x
    return parser.parse(x)


def ensure_date(x: Optional[Union[str, date]]) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    return parser.parse(x).date()


def ensure_time(x: Optional[Union[str, time]]) -> Optional[time]:
    if x is None:
        return None
    if isinstance(x, time):
        return x
    return parser.parse(x).time()


def ensure_bool(x: Optional[Union[str, bool]]) -> Optional[bool]:
    if x is None:
        return None
    if isinstance(x, str):
        x = x.lower()
        if x in ("t", "true"):
            return True
        if x in ("f", "false"):
            return False
        raise ValueError(x)
    elif isinstance(x, bool):
        return x
    raise TypeError(x)


def get_spinner(**kwargs):
    args = {
        "spinner": {"interval": 225, "frames": ["●∙∙", "∙●∙", "∙∙●", "∙●∙"]},
        "color": "white",
    }
    args.update(**kwargs)
    return Halo(**args)


success_symbol = " ✔ "
error_symbol = " ✖ "

cf = Colorful()
cf_palette = {
    "ghostWhite": "#F8F8F0",
    "lightGhostWhite": "#F8F8F2",
    "lightGray": "#CCCCCC",
    "gray": "#888888",
    "brownGray": "#49483E",
    "darkGray": "#282828",
    "yellow": "#E6DB74",
    "blue": "#66D9EF",
    "magenta": "#F92672",
    "purple": "#AE81FF",
    "brown": "#75715E",
    "orange": "#FD971F",
    "lightOrange": "#FFD569",
    "green": "#A6E22E",
    "seaGreen": "#529B2F",
}
cf_semantic_palette = {
    # "red": "#F92672",
    # "error": "#FD971F",
    "error": "#D83925",
    # "success": "#A6E22E",
    "info": "#66D9EF",
    "warning": "#FD971F",
    # "error": "#F8F8F0",
    "success": "#F8F8F0",
}
colors = list(cf_palette)
cf_palette.update(cf_semantic_palette)
cf.update_palette(cf_palette)


cycle_colors: Dict[str, str] = {}
fifo: List[str] = []


def reset_color_cycle():
    cycle_colors.clear()


def cycle_colors_unique(v: str) -> str:
    clr = None
    if v in cycle_colors:
        clr = cycle_colors[v]
    else:
        fifo.append(v)
        if len(fifo) >= len(colors):
            old_v = fifo.pop(0)
            if old_v in cycle_colors:
                clr = cycle_colors.pop(old_v)
    if clr is None:
        clr = list(set(colors) - set(cycle_colors.values()))[0]
    cycle_colors[v] = clr
    return getattr(cf, cycle_colors[v])(v)


def is_aware(d: Union[datetime, time]) -> bool:
    return d.tzinfo is not None and d.tzinfo.utcoffset(None) is not None


def date_to_str(dt: Union[str, date, datetime], date_format: str = "%F %T") -> str:
    if isinstance(dt, str):
        return dt
    return dt.strftime(date_format)


def _get_duration_components(duration: timedelta) -> Tuple[int, int, int, int, int]:
    """From Django"""
    days = duration.days
    seconds = duration.seconds
    microseconds = duration.microseconds

    minutes = seconds // 60
    seconds = seconds % 60

    hours = minutes // 60
    minutes = minutes % 60

    return days, hours, minutes, seconds, microseconds


def duration_iso_string(duration: timedelta) -> str:
    """From Django"""
    if duration < timedelta(0):
        sign = "-"
        duration *= -1
    else:
        sign = ""

    days, hours, minutes, seconds, microseconds = _get_duration_components(duration)
    ms = ".{:06d}".format(microseconds) if microseconds else ""
    return "{}P{}DT{:02d}H{:02d}M{:02d}{}S".format(
        sign, days, hours, minutes, seconds, ms
    )


def as_aware_datetime(v: Union[datetime, str, int]) -> datetime:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=pytz.UTC)
        return v
    if isinstance(v, str):
        dt = parser.parse(v)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=pytz.UTC)
        return dt
    if isinstance(v, int):
        return datetime.fromtimestamp(v, pytz.utc)
    return v


def is_datetime_str(s: str) -> bool:
    if len(s) < 8:
        # dateutil.parse is quite greedy, will take any integer like
        # thing as a date, "199603" for instance
        # So we assume shorter strings are not ever valid datetimes
        # Must be at least "1/1/1985" long
        return False
    try:
        parser.parse(s)
    except Exception:
        return False
    return True


class JSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        # kwargs["ignore_nan"] = True
        super().__init__(*args, **kwargs)

    def default(self, o: Any) -> str:
        # See "Date Time String Format" in the ECMA-262 specification.
        if isinstance(o, datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith("+00:00"):
                r = r[:-6] + "Z"
            return r
        elif isinstance(o, date):
            return o.isoformat()
        elif isinstance(o, time):
            if is_aware(o):
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, timedelta):
            return duration_iso_string(o)
        elif isinstance(o, (decimal.Decimal, uuid.UUID)):
            return str(o)
        else:
            return super().default(o)


class BasisJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> str:
        # See "Date Time String Format" in the ECMA-262 specification.
        if isinstance(o, datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith("+00:00"):
                r = r[:-6] + "Z"
            return r
        elif isinstance(o, date):
            return o.isoformat()
        elif isinstance(o, time):
            if is_aware(o):
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, timedelta):
            return duration_iso_string(o)
        elif isinstance(o, (decimal.Decimal, uuid.UUID)):
            return str(o)
        elif hasattr(o, "to_json"):
            return o.to_json()
        elif isinstance(o, StringEnum):
            return str(o)
        else:
            return super().default(o)


def to_json(d: Any) -> str:
    return json.dumps(d, cls=BasisJSONEncoder)
