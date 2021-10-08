from __future__ import annotations

from basis import function, Context, Record, Table

@function(
    inputs=[Record("streaming_input"), Table("aggregate_input")], # Example streaming and aggregate inputs 
    outputs=[Record()] # Defaults to "stdout" name
)
def {{ cookiecutter.function_name }}(
    ctx: Context,
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
    for record in ctx.get_records("streaming_input"):
        # Do somethihng
    # TODO: fill this in!
