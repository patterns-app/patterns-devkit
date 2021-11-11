from typing import List, Iterator


class InputTableMethods:
    @classmethod
    def as_dataframe(cls):
        ...


class OutputTableMethods:
    @classmethod
    def write(cls, data: List[dict]):
        ...


class InputStreamMethods:
    @classmethod
    def records(cls) -> Iterator[dict]:
        ...


class OutputStreamMethods:
    @classmethod
    def write_records(cls):
        ...


class ParameterMethods:
    @classmethod
    def get(cls):
        ...
