from __future__ import annotations

from typing import Iterator


class TestInputTable:
    def __init__(self, records: list[dict] = None, **kwargs):
        self.records = records
        for k, v in kwargs.items():
            setattr(self, k, v)

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
    def __init__(self, records: list[dict] = None, **kwargs):
        self.records = records
        for k, v in kwargs.items():
            setattr(self, k, v)

    def records(self) -> Iterator[dict]:
        return self.records

    def __iter__(self) -> Iterator[dict]:
        yield from self.records


class TestOutputTable:
    def __init__(self, **kwargs):
        self.records = None
        for k, v in kwargs.items():
            setattr(self, k, v)

    def create_table(self, records):
        self.records = records

    def write_records(self, records, replace=False):
        if replace:
            self.create_table(records)
        else:
            if self.records is None:
                self.records = []
            self.records.extend(records)

    def write_dataframe(self, dataframe, replace=False):
        records = dataframe.to_dict("records")
        self.write_records(records, replace=replace)


class TestOutputStream:
    def __init__(self, **kwargs):
        self.records = []
        for k, v in kwargs.items():
            setattr(self, k, v)

    def stream_record(self, record):
        self.records.append(record)

    def append_record(self, record):
        self.records.append(record)

    def append_records(self, records):
        self.records.extend(records)


class TestState:
    def __init__(self, **kwargs):
        self.state = {}
        for k, v in kwargs.items():
            setattr(self, k, v)

    def get_state_value(self, k):
        return self.state.get(k)

    def set_state_value(self, k, v):
        self.state[k] = v
