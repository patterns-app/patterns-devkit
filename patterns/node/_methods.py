from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Any

try:
    # DataFrame is only used in type annotations
    from pandas import DataFrame
except ImportError:
    DataFrame = None


class TableVersion:
    @property
    def name(self) -> str:
        ...

    @property
    def storage(self):
        ...

    @property
    def schema(self):
        ...

    @property
    def record_count(self) -> int | None:
        ...


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
    def get_active_version(cls) -> TableVersion | None:
        ...

    @classmethod
    def has_active_version(cls) -> bool:
        ...


class OutputTableMethods:
    @classmethod
    def write(cls, records: DataFrame | list[dict] | dict, replace=False):
        ...

    @classmethod
    def execute_sql(cls, sql: str, created: bool = False):
        ...

    @classmethod
    def create_new_version(cls) -> TableVersion:
        ...

    @classmethod
    def get_active_version(cls) -> TableVersion | None:
        ...

    @classmethod
    def set_active_version(cls, table: TableVersion):
        ...

    @classmethod
    def has_active_version(cls) -> bool:
        ...

    @classmethod
    def signal_create(cls):
        ...

    @classmethod
    def signal_update(cls):
        ...

    @classmethod
    def signal_reset(cls):
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
    def get_value(cls, key: str, default: Any = None) -> Any:
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
