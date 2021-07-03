from __future__ import annotations

from basis import DataBlock, datafunction, Context

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
    """Example docstring format. Short description goes here.

    This is the long description. *Accepts
    [markdown](https://daringfireball.net/projects/markdown/) formatting*.

    Inputs:
        input: Description of this input
    
    Params:
        param1: Help text for this param

    Output:
        Description of output
    """
    df = input.as_dataframe() # Or .as_records()
    return df
