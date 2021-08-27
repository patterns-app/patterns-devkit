from __future__ import annotations
from basis.core.execution.context import Context


import pytest
from basis.core.declarative.function import (
    BlockType,
    Parameter,
    Record,
    Table,
)
from basis.core.function import (
    make_function_from_simple_function,
    function_interface_from_callable,
    function,
)


def function_simple_generate():
    return {"a": 1}


def function_simple_transform(record):
    record["a"] = 1
    return record


@function(
    name="function_full_name",
    inputs=[Record("input1"), Table("input2")],
    outputs=[Record("output1"), Table("output2")],
    parameters=[Parameter("param1", "int")],
)
def function_full(ctx: Context):
    pass


def test_function_interface():
    interface = function_interface_from_callable(function_simple_generate)
    assert len(interface.inputs) == 0
    assert len(interface.outputs) == 1
    interface = function_interface_from_callable(function_simple_transform)
    assert len(interface.inputs) == 1
    i = interface.inputs["record"]
    assert i.name == "record"
    assert i.block_type == BlockType.Generic
    assert len(interface.outputs) == 1


def test_function_wrapper():
    fn = make_function_from_simple_function(function_simple_generate)
    assert fn.name == "function_simple_generate"
    assert len(fn.inputs) == 0
    assert len(fn.outputs) == 1
    assert len(fn.parameters) == 0
    fn = make_function_from_simple_function(function_simple_transform)
    assert fn.name == "function_simple_transform"
    assert len(fn.inputs) == 1
    assert len(fn.outputs) == 1
    assert len(fn.parameters) == 0


def test_function_decorator():
    fn = function_full
    assert fn.name == "function_full_name"
    assert len(fn.inputs) == 2
    assert len(fn.outputs) == 2
    assert len(fn.parameters) == 1

