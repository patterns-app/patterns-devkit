from __future__ import annotations


import pytest
from basis.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    BlockType,
    FunctionInterfaceCfg,
    Parameter,
)
from basis.core.function import (
    FunctionLike,
    datafunction,
    wrap_bare_function,
    function_interface_from_callable,
)


def function_simple_generate():
    return {"a": 1}


def function_simple_transform(record):
    record["a"] = 1
    return record


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
    wrapped = wrap_bare_function(function_simple_generate)
    wrapped = wrap_bare_function(function_simple_transform)
