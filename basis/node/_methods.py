from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Any

try:
    # DataFrame is only used in type annotations
    from pandas import DataFrame
except ImportError:
    DataFrame = None


@dataclass
class Table:
    name: str


class InputTableMethods:
    @classmethod
    def read(
        cls,
        as_format: str = "records",
        chunksize: int | None = None,
    ) -> list[dict] | DataFrame | Iterator[list[dict]] | Iterator[DataFrame]:
        ...

    @classmethod
    def read_sql(
        cls,
        sql: str,
        as_format: str = "records",
        chunksize: int | None = None,
    ) -> list[dict] | DataFrame | Iterator[list[dict]] | Iterator[DataFrame]:
        ...

    @classmethod
    def get_current_table(cls) -> Table | None:
        ...

    @classmethod
    def exists(cls) -> bool:
        ...


class OutputTableMethods:
    @classmethod
    def write(cls, records: DataFrame | list[dict] | dict, replace=False):
        ...

    @classmethod
    def execute_sql(cls, sql: str, created: bool = False):
        ...

    @classmethod
    def new_table(cls) -> Table:
        ...

    @classmethod
    def get_current_table(cls) -> Table | None:
        ...

    @classmethod
    def set_current_table(cls, table: Table):
        ...

    @classmethod
    def signal_create(cls):
        ...

    @classmethod
    def signal_update(cls):
        ...

    @classmethod
    def exists(cls) -> bool:
        ...


class InputStreamMethods:
    @classmethod
    def consume_records(cls, with_metadata: bool = False) -> Iterator[dict]:
        ...

    def __iter__(cls) -> Iterator[dict]:
        ...

    @classmethod
    def checkpoint(cls):
        ...

    @classmethod
    def rollback(cls, records_to_rollback: int = 1):
        ...

    @classmethod
    def consume_all(cls) -> Iterator[str | None]:
        ...


class OutputStreamMethods:
    @classmethod
    def write(cls, record: DataFrame | list[dict] | dict):
        ...


class StateMethods:
    @classmethod
    def set(cls, state: dict):
        ...

    @classmethod
    def set_value(cls, key: str, value: Any):
        ...

    @classmethod
    def get(cls) -> dict:
        ...

    @classmethod
    def get_value(cls, key: str) -> Any:
        ...

    @classmethod
    def should_continue(
        cls, pct_of_limit: float = None, seconds_till_limit: int = None
    ) -> bool:
        ...

    @classmethod
    def request_new_run(
        cls, trigger_downstream: bool = True, wait_atleast_seconds: int = None
    ):
        ...

class ParameterMethods:
    pass
