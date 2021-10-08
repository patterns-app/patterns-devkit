from __future__ import annotations

from basis import function, Context, Block


@function(
    inputs=[Table("input", Table("input")]
# @Param("param1", datatype="int")
)
def {{ cookiecutter.function_name }}(ctx: Context):
    df = input.as_dataframe() # Or .as_records()
    return df
