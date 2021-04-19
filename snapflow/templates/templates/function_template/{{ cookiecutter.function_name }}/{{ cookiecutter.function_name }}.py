from __future__ import annotations

from snapflow import DataBlock, Function, FunctionContext


# Add inputs and params
# @Input("input", stream=True)
# @Param("param1", datatype="int")
@Function(namespace="{{ cookiecutter.namespace }}")
def {{ cookiecutter.function_name }}(ctx: FunctionContext, input: DataBlock):
    df = input.as_dataframe() # Or .as_records()
    return df
