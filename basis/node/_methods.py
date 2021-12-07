from __future__ import annotations

from pandas import DataFrame
from typing import Iterator, Any


class InputTableMethods:
    @classmethod
    def as_dataframe(cls) -> DataFrame:
        ...

    @classmethod
    def as_records(cls) -> list[dict]:
        ...

    @classmethod
    def sql(cls, sql: str) -> OutputTableMethods:
        ...

    @classmethod
    def chunks(cls, chunk_size: int) -> OutputTableMethods:
        ...


class OutputTableMethods:
    @classmethod
    def create_table(cls, data: list[dict] | DataFrame):
        ...


class InputStreamMethods:
    @classmethod
    def records(cls) -> Iterator[dict]:
        ...

    def __iter__(self) -> Iterator[dict]:
        ...


class OutputStreamMethods:
    @classmethod
    def append_record(cls, record: dict):
        ...

    @classmethod
    def append_records(cls, records: list[dict]):
        ...


class StateMethods:
    def set_state(self, state: dict):
        ...

    def set_state_value(self, key: str, value: Any):
        ...

    def get_state(self) -> dict:
        ...

    def get_state_value(self, key: str) -> Any:
        ...


class ParameterMethods:
    pass
