from typing import Any, Callable, Dict, Iterable, Optional, Union

from basis.core.persistence.pydantic import BlockMetadataCfg

BlockStream = Iterable[BlockMetadataCfg]


def latest(stream: BlockStream) -> BlockStream:
    latest_: Optional[BlockMetadataCfg] = None
    for block in stream:
        latest_ = block
    if latest_ is None:
        return
    yield latest_


def one(stream: BlockStream) -> BlockStream:
    for b in stream:
        yield b
        break


# TODO: would be handy to have correlated join?
# def join(*streams: BlockStream, join_function: Callable[]) -> BlockStream:
#     for stream in streams:
#         for db in stream:
#             yield db


def merge(*streams: BlockStream) -> BlockStream:
    dbs = [db for s in streams for db in s]
    for db in sorted(dbs, key=lambda db: db.id):
        yield db


def filter(
    stream: BlockStream, function: Callable[[BlockMetadataCfg], bool]
) -> BlockStream:
    for db in stream:
        if function(db):
            yield db
