from __future__ import annotations

from typing import Iterator, Any

from pandas import DataFrame


class InputTableMethods:
    @classmethod
    def as_dataframe(cls, chunksize: int | None = None) -> DataFrame:
        ...

    @classmethod
    def as_records(cls, chunksize: int | None = None) -> list[dict]:
        ...

    @classmethod
    def execute_sql(cls, sql: str, **params) -> InputTableMethods:
        ...

    @classmethod
    def use_snapshot(cls, snapshot_name: str) -> InputTableMethods:
        ...


class OutputTableMethods:
    @classmethod
    def write_records(cls, records: list[dict] | DataFrame, replace: bool = False):
        ...

    @classmethod
    def create_snapshot(cls, snapshot_name: str):
        ...

    @classmethod
    def execute_sql(cls, sql: str, **params):
        ...


class InputStreamMethods:
    @classmethod
    def consume_records(cls, with_metadata: bool = False) -> Iterator[dict]:
        ...

    def __iter__(self) -> Iterator[dict]:
        ...

    @classmethod
    def checkpoint(cls):
        ...

    @classmethod
    def rollback(cls, records_to_rollback: int = 1):
        ...


class OutputStreamMethods:
    @classmethod
    def stream_record(cls, record: dict):
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
