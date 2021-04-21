from __future__ import annotations

from typing import Optional

from dcp.data_format.formats import (
    CsvFileObjectFormat,
    DataFrameFormat,
    JsonLinesFileObjectFormat,
)
from dcp.storage.base import Storage
from snapflow.core.execution.execution import DataFunctionContext
from snapflow.core.function import Input, Output, Param, datafunction
from snapflow.core.streams import Stream
from snapflow.utils.typing import T


@datafunction(namespace="core", display_name="Import CSV from Storage")
def import_storage_csv(
    ctx: DataFunctionContext, name: str, storage_url: str, schema: Optional[str] = None
):
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    fs_api = Storage(storage_url).get_api()
    f = fs_api.open_name(name)
    ctx.emit_state_value("imported", True)
    ctx.emit(f, data_format=CsvFileObjectFormat, schema=schema)
