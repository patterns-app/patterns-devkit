from __future__ import annotations

from typing import Optional

from dcp.storage.base import DatabaseStorageClass
from pandas import DataFrame, concat
from basis import DataBlock, DataFunctionContext
from basis.api import Stream
from basis.core.data_block import SelfReference
from basis.core.function import Input, Output, datafunction
from basis.modules.core.functions.accumulator.accumulator import accumulator
from basis.modules.core.functions.accumulator_sql.accumulator_sql import accumulator_sql
from basis.utils.typing import T


@datafunction(namespace="core", display_name="Accumulate successive outputs")
def accumulate(
    ctx: DataFunctionContext, input: Stream[T], previous: SelfReference[T] = None,
) -> DataBlock[T]:
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
