from __future__ import annotations

from snapflow import DataBlock, datafunction, Context

{% if cookiecutter.namespace %}
@datafunction(namespace="{{ cookiecutter.namespace }}")
{% else %}
@datafunction
{% endif %}
def {{ cookiecutter.function_name }}(
    ctx: Context,
    input: DataBlock
    # ref: Reference   # A reference input
    # param1: str = "default val"  # A parameter with default
):
    df = input.as_dataframe() # Or .as_records()
    return df
