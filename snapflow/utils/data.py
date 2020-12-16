from __future__ import annotations

import csv
import json
import typing
from datetime import datetime
from io import IOBase
from itertools import tee
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Union,
)

from loguru import logger
from pandas import Timestamp, isnull
from snapflow.utils.common import SnapflowJSONEncoder
from snapflow.utils.typing import T

if TYPE_CHECKING:
    from snapflow.core.data_formats import RecordsList


def records_list_as_dict_of_lists(dl: List[Dict]) -> Dict[str, List]:
    series: Dict[str, List] = {}
    for r in dl:
        for k, v in r.items():
            if k in series:
                series[k].append(v)
            else:
                series[k] = [v]
    return series


def is_nullish(o: Any, null_strings=["None", "null", "na", ""]) -> bool:
    # TOOD: is "na" too aggressive?
    if o is None:
        return True
    if isinstance(o, str):
        if o.lower() in null_strings:
            return True
    if isinstance(o, Iterable):
        # No basic python object is "nullish", even if empty
        return False
    if isnull(o):
        return True
    return False


class SnapflowCsvDialect(csv.Dialect):
    delimiter = ","
    quotechar = '"'
    escapechar = "\\"
    doublequote = True
    skipinitialspace = False
    quoting = csv.QUOTE_MINIMAL
    lineterminator = "\n"


def process_csv_value(v: Any) -> Any:
    if is_nullish(v):
        return None
    return v


def read_csv(lines: Iterable, dialect=SnapflowCsvDialect) -> List[Dict]:
    reader = csv.reader(lines, dialect=dialect)
    headers = next(reader)
    records = [
        {h: process_csv_value(v) for h, v in zip(headers, line)} for line in reader
    ]
    return records


def read_raw_string_csv(csv_str: str, **kwargs) -> List[Dict]:
    lines = [ln.strip() for ln in csv_str.split("\n") if ln.strip()]
    return read_csv(lines, **kwargs)


def conform_csv_value(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, list) or isinstance(v, dict):
        return json.dumps(v, cls=SnapflowJSONEncoder)
    return v


def write_csv(
    records: List[Dict],
    file_like: IOBase,
    columns: List[str] = None,
    append: bool = False,
    dialect=SnapflowCsvDialect,
):
    if not records:
        return
    writer = csv.writer(file_like, dialect=dialect)
    if not columns:
        columns = list(records[0].keys())  # Assumes all records have same keys...
    if not append:
        # Write header if not appending
        writer.writerow(columns)
    for record in records:
        row = []
        for c in columns:
            v = record.get(c)
            v = conform_csv_value(v)
            row.append(v)
        writer.writerow(row)


def read_json(j: str) -> Union[Dict, List]:
    return json.loads(j)  # TODO: de-serializer


def conform_records_for_insert(
    records: RecordsList,
    columns: List[str],
    adapt_objects_to_json: bool = True,
    conform_datetimes: bool = True,
):
    rows = []
    for r in records:
        row = []
        for c in columns:
            o = r.get(c)
            # TODO: this is some magic buried down here. no bueno
            if adapt_objects_to_json and (isinstance(o, list) or isinstance(o, dict)):
                o = json.dumps(o, cls=SnapflowJSONEncoder)
            if conform_datetimes:
                if isinstance(o, Timestamp):
                    o = o.to_pydatetime()
            row.append(o)
        rows.append(row)
    return rows


def head(file_obj: IOBase, n: int) -> Iterator:
    if not hasattr(file_obj, "seek"):
        raise TypeError("Missing seek method")
    i = 0
    for v in file_obj:
        if i >= n:
            break
        yield v
        i += 1
    file_obj.seek(0)


class SampleableIterator(Generic[T]):
    def __init__(
        self, iterator: typing.Iterator, iterated_values: Optional[List[T]] = None
    ):
        self._iterator = iterator
        self._iterated_values: List[T] = iterated_values or []
        self._i = 0
        self._has_been_iterated = False

    def __iter__(self) -> Iterator[T]:
        for v in self._iterated_values:
            yield v
        for v in self._iterator:
            yield v

    # def __next__(self) -> T:
    #     if self._i < len(self._iterated_values) - 1:
    #         self._i += 1
    #         return self._iterated_values[i]
    #     nxt = next(self._iterator)
    #     self._iterated_values.append(nxt)
    #     self._i += 1
    #     return nxt

    def get_first(self) -> Optional[T]:
        return next(self.head(1), None)

    def copy(self) -> SampleableIterator[T]:
        logger.warning("Copying an iterator, this requires bringing it into memory.")
        self._iterator, it2 = tee(self._iterator, 2)
        return SampleableIterator(it2, self._iterated_values)

    def head(self, n: int) -> Iterator[T]:
        i = 0
        for v in self._iterated_values:
            if i >= n:
                return
            yield v
            i += 1
        for v in self._iterator:
            if i >= n:
                return
            self._iterated_values.append(v)
            yield v
            i += 1

    def __getattr__(self, name: str) -> Any:
        return getattr(self._iterator, name)


class SampleableIO(SampleableIterator):
    def __init__(self, file_obj: IOBase):
        super().__init__(file_obj)

    def copy(self) -> SampleableIterator[T]:
        logger.warning("Cannot copy file object, reusing existing")
        return self
