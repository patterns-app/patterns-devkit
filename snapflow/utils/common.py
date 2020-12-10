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
from typing import Any, Dict, Generic, List, Optional, Set, Tuple, Type, Union

import pytz
from colorful import Colorful
from dateutil import parser
from snapflow.utils.typing import K, T, V

DEBUG = False


def printd(*o):
    if DEBUG:
        print(*[cf.dimmed(i) for i in o])


class AttrDict(Dict[K, V]):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


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


def as_identifier(s: str) -> str:
    # make db-compatible identifier from str
    s = re.sub(r"\W+", "_", s).lower().strip("_")
    if s and not re.match(r"[a-z_]", s[0]):
        s = "_" + s  # Must start with alpha
    return s


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


def dataclass_kwargs(dc: Any, kwargs: Dict) -> Dict:
    return {f.name: kwargs.get(f.name) for f in fields(dc)}


def remove_dupes(a: List[T]) -> List[T]:
    seen: Set[T] = set()
    deduped: List[T] = []
    for i in a:
        if i in seen:
            continue
        seen.add(i)
        deduped.append(i)
    return deduped


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
    "error": "#D83925",
    "info": "#66D9EF",
    "warning": "#FD971F",
    "success": "#A6E22E",
}
colors = list(cf_palette)
cf_palette.update(cf_semantic_palette)
cf.update_palette(cf_palette)


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
    """
    Relatively conservative datetime string detector. Takes preference
    for numerics over datetimes (so 20201201 is an integer, not a date)
    """
    if not isinstance(s, str):
        s = str(s)
    try:
        int(s)
        return False
    except (TypeError, ValueError):
        pass
    try:
        float(s)
        return False
    except (TypeError, ValueError):
        pass
    try:
        # We use ancient date as default to detect when no date was found
        # Will fail if trying to parse actual ancient dates!
        dt = parser.parse(s, default=datetime(1, 1, 1))
        if dt.year < 2:
            # dateutil parser only found a time, not a date
            return False
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


class DagsJSONEncoder(json.JSONEncoder):
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
    return json.dumps(d, cls=DagsJSONEncoder)


def profile_stmt(stmt: str, globals: Dict, locals: Dict):
    import cProfile
    import pstats
    from pstats import SortKey

    cProfile.runctx(
        stmt,
        globals=globals,
        locals=locals,
        filename="profile.stats",
    )
    p = pstats.Stats("profile.stats")
    p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats(100)
