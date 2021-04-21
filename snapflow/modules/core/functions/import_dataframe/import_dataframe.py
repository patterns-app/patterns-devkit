from __future__ import annotations

from typing import Optional

from dcp.data_format.formats import (
    CsvFileObjectFormat,
    DataFrameFormat,
    JsonLinesFileObjectFormat,
)
from pandas.core.frame import DataFrame
from snapflow.core.execution.execution import DataFunctionContext
from snapflow.core.function import Input, Output, Param, datafunction
from snapflow.core.streams import Stream


@datafunction(
    namespace="core",
    display_name="Import Pandas DataFrame",
)
def import_dataframe(
    ctx: DataFunctionContext, dataframe: str, schema: Optional[str] = None
):
    """
    Import pandas DataFrame

    dataframe:
    """
    imported = ctx.get_state_value("imported")
    if imported:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("imported", True)
    # schema = ctx.get_param("schema")
    # df = ctx.get_param("dataframe")
    ctx.emit(dataframe, data_format=DataFrameFormat, schema=schema)
