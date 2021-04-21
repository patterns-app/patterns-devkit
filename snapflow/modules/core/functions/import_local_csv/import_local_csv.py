from __future__ import annotations

from typing import Optional

from dcp.data_format.formats import (
    CsvFileObjectFormat,
    DataFrameFormat,
    JsonLinesFileObjectFormat,
)
from snapflow.core.execution.execution import DataFunctionContext
from snapflow.core.function import Input, Output, Param, datafunction
from snapflow.core.streams import Stream
from snapflow.utils.typing import T


@datafunction(namespace="core", display_name="Import local CSV")
def import_local_csv(ctx: DataFunctionContext, path: str, schema: Optional[str] = None):
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    f = open(path)
    ctx.emit_state_value("imported", True)
    ctx.emit(f, data_format=CsvFileObjectFormat, schema=schema)
