from __future__ import annotations
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


@Snap(module="core", display_name="Import local CSV")
@Param("path", datatype="str")
@Param("schema", datatype="str", required=False)
def import_local_csv(ctx: SnapContext):
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    path = ctx.get_param("path")
    f = open(path)
    ctx.emit_state_value("imported", True)
    schema = ctx.get_param("schema")
    ctx.emit(f, data_format=CsvFileObjectFormat, schema=schema)

