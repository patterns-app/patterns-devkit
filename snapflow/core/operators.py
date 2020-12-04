from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Dict, Optional, Union

from snapflow.core.data_block import DataBlock
from snapflow.core.node import ensure_stream
from snapflow.core.streams import DataBlockStream, StreamBuilder, StreamLike

OpCallable = Callable[[DataBlockStream], DataBlockStream]


@dataclass(frozen=True)
class BoundOperator:
    op_callable: OpCallable
    kwargs: Dict[str, Any]


@dataclass(frozen=True)
class Operator:
    op_callable: OpCallable

    def __call__(self, stream: StreamLike, **kwargs) -> StreamBuilder:
        stream = ensure_stream(stream)
        return stream.apply_operator(
            BoundOperator(op_callable=self.op_callable, kwargs=kwargs)
        )


# TODO: add parameter constraint (position-only)?
def operator(op: OpCallable = None, **kwargs) -> Union[Callable, Operator]:
    if op is None:
        return partial(operator, **kwargs)
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
def concat(*streams: DataBlockStream) -> DataBlockStream:
    for stream in streams:
        for db in stream:
            yield db


@operator
def merge(*streams: DataBlockStream) -> DataBlockStream:
    dbs = [db for s in streams for db in s]
    for db in sorted(dbs, key=lambda db: db.data_block_id):
        yield db


@operator
def filter(
    stream: DataBlockStream, function: Callable[[DataBlock], bool]
) -> DataBlockStream:
    for db in stream:
        if function(db):
            yield db
