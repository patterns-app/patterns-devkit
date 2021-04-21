from __future__ import annotations

from snapflow import DataFunction, DataFunctionContext, DataBlock


@DataFunction(namespace="{{ cookiecutter.namespace }}")
# @Input("input", stream=True)
# @Param("param1", datatype="int")
def {{ cookiecutter.function_name }}(ctx: DataFunctionContext, input: DataBlock):
    df = input.as_dataframe() # Or .as_records()
    return df
