from __future__ import annotations
from basis.core.declarative.function import Parameter

from typing import Optional

from basis import Context, function
from dcp.data_format.formats import DataFrameFormat, JsonLinesFileObjectFormat
from pandas.core.frame import DataFrame


@function(
    display_name="Import Pandas DataFrame",
    parameters=[
        Parameter("dataframe", "str", required=True),
        Parameter("schema", "str", required=False),
    ],
)
def import_dataframe(ctx: Context):
    """
    Import pandas DataFrame

    dataframe:
    """
    imported = ctx.get_state_value("imported")
    if imported:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("imported", True)
    schema = ctx.get_param("schema")
    df = ctx.get_param("dataframe")
    ctx.emit_table(df, data_format=DataFrameFormat, schema=schema)
