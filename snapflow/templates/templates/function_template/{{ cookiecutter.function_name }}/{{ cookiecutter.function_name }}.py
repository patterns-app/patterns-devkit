from __future__ import annotations

from snapflow import DataBlock, datafunction, DataFunctionContext


@datafunction(namespace="{{ cookiecutter.namespace }}")
def {{ cookiecutter.function_name }}(
    ctx: DataFunctionContext,
    input: DataBlock
    # ref: Reference   # A reference input
    # param1: str = "default val"  # A parameter with default
):
    df = input.as_dataframe() # Or .as_records()
    return df
