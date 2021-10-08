from __future__ import annotations

from basis import simple_streaming_component, simple_table_component


@simple_streaming_component
def comp1(
    input_record,
    # input_table  # <- Can optionally accept table inputs for reference
):
    """Example docstring format. Short description goes here.

    This is the long description. *Accepts
    [markdown](https://daringfireball.net/projects/markdown/) formatting*.

    Inputs:
        input_record: Description of this input
    
    Output:
        Description of output
    """
    # Do things with the record:
    input_record["new_value"] = 1.0
    # Return new or updated record:
    return input_record
