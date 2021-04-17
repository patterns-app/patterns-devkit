from __future__ import annotations
from snapflow.core.execution.execution import FunctionContext
from snapflow.core.streams import Stream

from typing import Optional

from dcp.data_format.formats import (
    CsvFileObjectFormat,
    DataFrameFormat,
    JsonLinesFileObjectFormat,
)
from snapflow.core.function import Input, Output, Param, Function


@Function(
    namespace="core", display_name="Import Pandas DataFrame",
)
@Param("dataframe", datatype="DataFrame")
@Param("schema", datatype="str", required=False)
def import_dataframe(ctx: FunctionContext):  # TODO optional
    imported = ctx.get_state_value("imported")
    if imported:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("imported", True)
    schema = ctx.get_param("schema")
    df = ctx.get_param("dataframe")
    ctx.emit(df, data_format=DataFrameFormat, schema=schema)
