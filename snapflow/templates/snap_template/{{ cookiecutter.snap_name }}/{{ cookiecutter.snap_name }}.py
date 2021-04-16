from __future__ import annotations

from snapflow import Snap, SnapContext, DataBlock


@Snap(namespace={{ cookiecutter.namespace }})
# @Input("input", stream=True)
# @Param("param1", datatype="int")
def {{ cookiecutter.snap_name }}(ctx: SnapContext, input: DataBlock):
    df = input.as_dataframe() # Or .as_records()
    return df
