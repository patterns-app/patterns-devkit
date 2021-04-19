from __future__ import annotations

from typing import Optional

from dcp.data_format.formats import (
    CsvFileObjectFormat,
    DataFrameFormat,
    JsonLinesFileObjectFormat,
)
from snapflow.core.execution.execution import FunctionContext
from snapflow.core.function import Function, Input, Output, Param
from snapflow.core.streams import Stream
from snapflow.utils.typing import T


@Function(namespace="core", display_name="Import local CSV")
def import_local_csv(ctx: FunctionContext, path: str, schema: Optional[str] = None):
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    f = open(path)
    ctx.emit_state_value("imported", True)
    ctx.emit(f, data_format=CsvFileObjectFormat, schema=schema)
