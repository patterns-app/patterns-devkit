from __future__ import annotations

import csv
import json
import typing
from datetime import datetime
from io import IOBase
from itertools import tee
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    AnyStr,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Union,
)

from loguru import logger
from pandas import Timestamp, isnull
from snapflow.utils.common import SnapflowJSONEncoder, title_to_snake_case
from snapflow.utils.typing import T
from sqlalchemy.engine.result import ResultProxy

if TYPE_CHECKING:
    from snapflow.storage.data_formats import Records


def records_as_dict_of_lists(dl: List[Dict]) -> Dict[str, List]:
    series: Dict[str, List] = {}
    for r in dl:
        for k, v in r.items():
            if k in series:
                series[k].append(v)
            else:
                series[k] = [v]
    return series


def is_nullish(
    o: Any, null_strings=["None", "null", "na", "", "NULL", "NA", "N/A"]
) -> bool:
    # TOOD: is "na" too aggressive?
    if o is None:
        return True
    if isinstance(o, str):
        if o in null_strings:
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


def process_raw_value(v: Any) -> Any:
    if is_nullish(v):
        return None
    return v


def clean_record(
    record: Dict[str, Any], ensure_keys_snake_case: bool = True
) -> Dict[str, Any]:
    if ensure_keys_snake_case:
        return {title_to_snake_case(k): process_raw_value(v) for k, v in record.items()}
    return {k: process_raw_value(v) for k, v in record.items()}


def ensure_strings(i: Iterator[AnyStr]) -> Iterator[str]:
    for s in i:
        if isinstance(s, bytes):
            s = s.decode("utf8")
        yield s


def iterate_chunks(iterator: Iterator[T], chunk_size: int) -> Iterator[List[T]]:
    chunk = []
    for v in iterator:
        chunk.append(v)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    yield chunk


def add_header(itr: Iterable[T], header: Dict[str, Optional[T]]) -> Iterable[T]:
    first = True
    for v in itr:
        if header["header"] is None:
            header["header"] = v  # "return" this header value back to caller
        elif first:
            yield header["header"]
        yield v
        first = False


def with_header(iterator: Iterator[Iterable[T]]) -> Iterator[Iterable[T]]:
    header = {"header": None}
    for chunk in iterator:
        chunk = add_header(chunk, header)
        yield chunk


def read_csv(lines: Iterable[AnyStr], dialect=SnapflowCsvDialect) -> Iterator[Dict]:
    lines = ensure_strings(lines)
    reader = csv.reader(lines, dialect=dialect)
    try:
        headers = next(reader)
    except StopIteration:
        return
    for line in reader:
        yield {h: process_raw_value(v) for h, v in zip(headers, line)}


def read_raw_string_csv(csv_str: str, **kwargs) -> Iterator[Dict]:
    lines = [ln.strip() for ln in csv_str.split("\n") if ln.strip()]
    return read_csv(lines, **kwargs)


def conform_to_csv_value(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, list) or isinstance(v, dict):
        return json.dumps(v, cls=SnapflowJSONEncoder)
    return v


def write_csv(
    records: List[Dict],
    file_like: IO,
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
            v = conform_to_csv_value(v)
            row.append(v)
        writer.writerow(row)


def read_json(j: str) -> Union[Dict, List]:
    return json.loads(j)  # TODO: de-serializer


def conform_records_for_insert(
    records: Records,
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
        self._is_used = False

    def __iter__(self) -> Iterator[T]:
        if self._is_used:
            raise Exception("Iterator already used")  # TODO: better exception
        for v in self._iterated_values:
            yield v
        for v in self._iterator:
            self._is_used = True
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
        from snapflow.storage.data_records import wrap_records_object

        if n < 1:
            return
        i = 0
        for v in self._iterated_values:
            yield v
            i += 1
            if i >= n:
                return
        for v in self._iterator:
            # Important: we are uncovering a new records object potentially
            # so we must wrap it immediately
            wrapped_v = wrap_records_object(v)
            self._iterated_values.append(wrapped_v)
            yield wrapped_v
            i += 1
            if i >= n:
                return

    def __getattr__(self, name: str) -> Any:
        return getattr(self._iterator, name)


class SampleableIO(SampleableIterator):
    def __init__(self, file_obj: IOBase):
        super().__init__(file_obj)

    def copy(self) -> SampleableIterator[T]:
        logger.warning("Cannot copy file object, reusing existing")
        return self


class SampleableCursor(SampleableIterator):
    def __init__(self, cursor: ResultProxy):
        super().__init__(cursor)

    def copy(self) -> SampleableIterator[T]:
        logger.warning("Cannot copy cursor object, reusing existing")
        return self


class SampleableGenerator(SampleableIterator):
    def __init__(self, gen: Generator):
        super().__init__(gen)
