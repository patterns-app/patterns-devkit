from __future__ import annotations

from typing import Optional

from basis import Block, Context
from basis.api import Stream
from basis.core.block import SelfReference
from basis.core.function import Input, Output, function
from basis.modules.core.functions.accumulator.accumulator import accumulator
from basis.modules.core.functions.accumulator_sql.accumulator_sql import accumulator_sql
from basis.utils.typing import T
from dcp.storage.base import DatabaseStorageClass
from pandas import DataFrame, concat


@function(namespace="core", display_name="Accumulate successive outputs")
def accumulate(
    ctx: Context,
    input: Stream[T],
    previous: SelfReference[T] = None,
) -> Block[T]:
    """Adaptive to storages.
    TODO: is this the right pattern for handling different storage classes / engines? No probably not,
    but good hack for now, lots of flexibility.
    TODO: how to specify _supported_ storage classes vs _required_ storage classes / engines?
    """
    if (
        ctx.execution_cfg.get_target_storage().storage_engine.storage_class
        == DatabaseStorageClass
    ):
        return accumulator_sql(ctx, input, previous)
    return accumulator(input, previous)
