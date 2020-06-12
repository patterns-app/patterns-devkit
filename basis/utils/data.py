from __future__ import annotations

import csv
from typing import Any, Dict, Iterable, List

from pandas import isnull


def records_list_as_listdicts(dl: List[Dict]) -> Dict[str, List]:
    series = {}
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


class BasisCsvDialect(csv.Dialect):
    delimiter = ","
    quotechar = '"'
    escapechar = "/"
    doublequote = True
    skipinitialspace = False
    quoting = csv.QUOTE_MINIMAL
    lineterminator = "\n"


def process_csv_value(v: Any) -> Any:
    if is_nullish(v):
        return None
    return v


def read_csv(lines: Iterable, dialect=BasisCsvDialect) -> List[Dict]:
    reader = csv.reader(lines, dialect=dialect)
    headers = next(reader)
    records = [
        {h: process_csv_value(v) for h, v in zip(headers, line)} for line in reader
    ]
    return records
