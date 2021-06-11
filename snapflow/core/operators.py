from typing import Any, Callable, Dict, Iterable, Optional, Union

from snapflow.core.persistence.pydantic import DataBlockMetadataCfg

DataBlockStream = Iterable[DataBlockMetadataCfg]


def latest(stream: DataBlockStream) -> DataBlockStream:
    latest_: Optional[DataBlockMetadataCfg] = None
    for block in stream:
        latest_ = block
    if latest_ is None:
        return
    yield latest_


def one(stream: DataBlockStream) -> DataBlockStream:
    for b in stream:
        yield b
        break


# TODO: would be handy to have correlated join?
# def join(*streams: DataBlockStream, join_function: Callable[]) -> DataBlockStream:
#     for stream in streams:
#         for db in stream:
#             yield db


def merge(*streams: DataBlockStream) -> DataBlockStream:
    dbs = [db for s in streams for db in s]
    for db in sorted(dbs, key=lambda db: db.id):
        yield db


def filter(
    stream: DataBlockStream, function: Callable[[DataBlockMetadataCfg], bool]
) -> DataBlockStream:
    for db in stream:
        if function(db):
            yield db
