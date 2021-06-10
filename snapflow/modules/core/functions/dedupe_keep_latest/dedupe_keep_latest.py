from __future__ import annotations

from dcp.storage.base import DatabaseStorageClass
from pandas import DataFrame
from snapflow import DataBlock, DataFunctionContext
from snapflow.core.function import datafunction
from snapflow.modules.core.functions.dedupe_keep_latest_dataframe.dedupe_keep_latest_dataframe import (
    dedupe_keep_latest_dataframe,
)
from snapflow.modules.core.functions.dedupe_keep_latest_sql.dedupe_keep_latest_sql import (
    dedupe_keep_latest_sql,
)
from snapflow.utils.typing import T

# TODO: currently no-op when no unique columns specified.
#  In general any deduping on non-indexed columns will be costly.


@datafunction(
    namespace="core",
    display_name="Dedupe records (keep latest)",
)
def dedupe_keep_latest(ctx: DataFunctionContext, input: DataBlock[T]) -> DataFrame[T]:
    """Adaptive to storages.
    TODO: is this the right pattern for handling different storage classes / engines? No probably not,
    but good hack for now, lots of flexibility.
    TODO: how to specify _supported_ storage classes vs _required_ storage classes / engines?
    """
    if (
        ctx.execution_config.get_target_storage().storage_engine.storage_class
        == DatabaseStorageClass
    ):
        return dedupe_keep_latest_sql(ctx, input)
    return dedupe_keep_latest_dataframe(input)
