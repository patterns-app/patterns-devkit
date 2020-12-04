from __future__ import annotations

import csv
import json
from datetime import datetime
from io import IOBase
from typing import Any, Dict, Iterable, List, Union

from pandas import Timestamp, isnull
from snapflow.core.data_formats import RecordsList
from snapflow.utils.common import DagsJSONEncoder


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


class DagsCsvDialect(csv.Dialect):
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


def read_csv(lines: Iterable, dialect=DagsCsvDialect) -> List[Dict]:
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
        return json.dumps(v, cls=DagsJSONEncoder)
    return v


def write_csv(
    records: List[Dict],
    file_like: IOBase,
    columns: List[str] = None,
    append: bool = False,
    dialect=DagsCsvDialect,
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
                o = json.dumps(o, cls=DagsJSONEncoder)
            if conform_datetimes:
                if isinstance(o, Timestamp):
                    o = o.to_pydatetime()
            row.append(o)
        rows.append(row)
    return rows
