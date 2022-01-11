from __future__ import annotations

from typing import Iterator


class TestInputTable:
    def __init__(self, records: list[dict] = None):
        self.records = records

    def as_dataframe(self):
        import pandas

        return pandas.DataFrame.from_records(self.records)

    def as_records(self) -> list[dict]:
        return self.records

    def sql(self, sql: str):
        raise NotImplementedError

    def chunks(self, chunk_size: int):
        raise NotImplementedError


class TestInputStream:
    def __init__(self, records: list[dict] = None):
        self.records = records

    def records(self) -> Iterator[dict]:
        return self.records

    def __iter__(self) -> Iterator[dict]:
        yield from self.records


class TestOutputTable:
    def __init__(self):
        self.table = None

    def create_table(self, table):
        self.table = table


class TestOutputStream:
    def __init__(self):
        self.records = []

    def append_record(self, record):
        self.records.append(record)

    def append_records(self, records):
        self.records.extend(records)


class TestState:
    def __init__(self):
        self.state = {}

    def get_state_value(self, k):
        return self.state.get(k)

    def set_state_value(self, k, v):
        self.state[k] = v
