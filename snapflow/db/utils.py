from collections.abc import Generator
from typing import Iterable, List

from snapflow.core.data_formats import RecordsList
from sqlalchemy.engine import ResultProxy, RowProxy


def result_proxy_to_records_list(
    result_proxy: ResultProxy, rows: Iterable[RowProxy] = None
) -> RecordsList:
    if not result_proxy:
        return []
    if not rows:
        rows = result_proxy
    return [{k: v for k, v in zip(result_proxy.keys(), row)} for row in rows]


def db_result_batcher(result_proxy: ResultProxy, batch_size: int = 1000) -> Generator:
    while True:
        rows = result_proxy.fetchmany(batch_size)
        yield result_proxy_to_records_list(result_proxy, rows)
        if len(rows) < batch_size:
            return
