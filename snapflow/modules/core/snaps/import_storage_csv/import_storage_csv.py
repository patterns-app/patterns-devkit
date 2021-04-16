from __future__ import annotations

from dcp.storage.base import Storage
from snapflow.core.execution.execution import SnapContext
from snapflow.core.streams import Stream

from typing import Optional

from dcp.data_format.formats import (
    CsvFileObjectFormat,
    DataFrameFormat,
    JsonLinesFileObjectFormat,
)
from snapflow.core.snap import Input, Output, Param, Snap

from snapflow.utils.typing import T


@Snap(namespace="core", display_name="Import CSV from Storage")
@Param("name", datatype="str")
@Param("storage_url", datatype="str")
@Param("schema", datatype="str", required=False)
def import_storage_csv(ctx: SnapContext):
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    name = ctx.get_param("name")
    storage_url = ctx.get_param("storage_url")
    fs_api = Storage(storage_url).get_api()
    f = fs_api.open_name(name)
    ctx.emit_state_value("imported", True)
    schema = ctx.get_param("schema")
    ctx.emit(f, data_format=CsvFileObjectFormat, schema=schema)
