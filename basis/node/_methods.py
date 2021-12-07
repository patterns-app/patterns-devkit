from __future__ import annotations

from pandas import DataFrame
from typing import Iterator


class InputTableMethods:
    @classmethod
    def as_dataframe(cls) -> DataFrame:
        ...

    @classmethod
    def as_records(cls) -> list[dict]:
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


<<<<<<< Updated upstream
class StateMethods:
    @classmethod
    def records(cls) -> Iterator[dict]:
        ...

    @classmethod
    def write_records(cls):
        ...
=======
class ParameterMethods:
    pass


>>>>>>> Stashed changes
