from __future__ import annotations

from pandas import DataFrame, concat
from snapflow.core.data_block import SelfReference, Stream
from snapflow.core.function import datafunction
from snapflow.utils.typing import T


@datafunction(namespace="core", display_name="Accumulate DataFrames")
def accumulator(
    input: Stream[T],
    previous: SelfReference[T] = None,
) -> DataFrame[T]:
    # TODO: make this return a dataframe iterator right?
    accumulated_dfs = [block.as_dataframe() for block in input]
    if previous is not None:
        accumulated_dfs = [previous.as_dataframe()] + accumulated_dfs
    return concat(accumulated_dfs)
