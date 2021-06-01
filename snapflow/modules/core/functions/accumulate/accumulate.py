from __future__ import annotations

from typing import Optional

from dcp.storage.base import DatabaseStorageClass
from pandas import DataFrame, concat
from snapflow.core.data_block import DataBlock, SelfReference
from snapflow.core.execution import DataFunctionContext
from snapflow.core.function import Input, Output, datafunction
from snapflow.core.streams import Stream
from snapflow.modules.core.functions.accumulator.accumulator import accumulator
from snapflow.modules.core.functions.accumulator_sql.accumulator_sql import (
    accumulator_sql,
)
from snapflow.utils.typing import T


@datafunction(namespace="core", display_name="Accumulate successive outputs")
def accumulate(
    ctx: DataFunctionContext,
    input: Stream[T],
    previous: SelfReference[T] = None,
) -> T:
    """Adaptive to storages.
    TODO: is this the right pattern for handling different storage classes / engines? No probably not,
    but good hack for now, lots of flexibility.
    TODO: how to specify _supported_ storage classes vs _required_ storage classes / engines?
    """
    if (
        ctx.execution_config.get_target_storage().storage_engine.storage_class
        == DatabaseStorageClass
    ):
        return accumulator_sql(ctx, input, previous)
    return accumulator(input, previous)
