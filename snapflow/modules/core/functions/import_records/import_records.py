from __future__ import annotations

from typing import Optional

from dcp.data_format.formats import (
    CsvFileObjectFormat,
    DataFrameFormat,
    JsonLinesFileObjectFormat,
)
from dcp.data_format.formats.memory.records import RecordsFormat
from snapflow.core.execution.execution import FunctionContext
from snapflow.core.function import Function, Input, Output, Param
from snapflow.core.streams import Stream


@Function(
    namespace="core",
    display_name="Import Records (List of dicts)",
)
def import_records(ctx: FunctionContext, records: str, schema: Optional[str] = None):
    imported = ctx.get_state_value("imported")
    if imported:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("imported", True)
    ctx.emit(records, data_format=RecordsFormat, schema=schema)
