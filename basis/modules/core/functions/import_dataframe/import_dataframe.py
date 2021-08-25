from __future__ import annotations

from typing import Optional

from basis import Context, function
from dcp.data_format.formats import DataFrameFormat, JsonLinesFileObjectFormat
from pandas.core.frame import DataFrame


@function(
    namespace="core", display_name="Import Pandas DataFrame",
)
def import_dataframe(ctx: Context, dataframe: str, schema: Optional[str] = None):
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
