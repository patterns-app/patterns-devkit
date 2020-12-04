from dataclasses import dataclass
from functools import partial
from typing import Callable, Dict, Optional

from dags import ObjectSchema
from dags.core.data_block import DataBlock
from dags.core.node import ensure_stream
from dags.core.runnable import ExecutionContext
from dags.core.streams import (
    DataBlockStream,
    ManagedDataBlockStream,
    StreamBuilder,
    StreamLike,
)

OpCallable = Callable[[DataBlockStream], DataBlockStream]


@dataclass(frozen=True)
class Operator:
    op_callable: OpCallable

    def __call__(self, stream: StreamLike) -> StreamBuilder:
        stream = ensure_stream(stream)
        return stream.apply_operator(self)


def operator(op: OpCallable) -> Operator:
    return Operator(op_callable=op)


@operator
def latest(stream: DataBlockStream) -> DataBlockStream:
    latest_: Optional[DataBlock] = None
    for block in stream:
        latest_ = block
    if latest_ is None:
        return
    yield latest_


@operator
def one(stream: DataBlockStream) -> DataBlockStream:
    yield next(stream)


@operator
def join(*streams: DataBlockStream) -> DataBlockStream:
    for stream in streams:
        for db in stream:
            yield db
