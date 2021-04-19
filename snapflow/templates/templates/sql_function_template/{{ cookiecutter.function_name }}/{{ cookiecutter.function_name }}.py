from __future__ import annotations

from snapflow import DataBlock, Function, FunctionContext


@Function(namespace="{{ cookiecutter.namespace }}")
# @Input("input", stream=True)
# @Param("param1", datatype="int")
def {{ cookiecutter.function_name }}(ctx: FunctionContext, input: DataBlock):
    df = input.as_dataframe() # Or .as_records()
    return df
