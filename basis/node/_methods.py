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
    def sql(cls, sql: str) -> InputTableMethods:
        ...

    @classmethod
    def chunks(cls, chunk_size: int) -> InputTableMethods:
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
    @classmethod
    def set_state(cls, state: dict):
        ...

    @classmethod
    def set_state_value(cls, key: str, value: Any):
        ...

    @classmethod
    def get_state(cls) -> dict:
        ...

    @classmethod
    def get_state_value(cls, key: str) -> Any:
        ...


class ParameterMethods:
    pass
