from __future__ import annotations

import logging
from pathlib import Path
from pprint import pprint

from loguru import logger
from snapflow import DataFunction, datafunction
from snapflow.core.environment import Environment
from snapflow.core.execution.execution import DataFunctionContext
from snapflow.core.function_interface import DataFunctionInterface
from snapflow.core.function_package import DataFunctionPackage
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.modules import core

logger.enable("snapflow")


def test_from_path():
    pth = Path(__file__).resolve().parents[0] / "_test_module/functions/test_function"
    mffunction = DataFunctionPackage.from_path(str(pth))
    function = mffunction.function
    assert function.name == mffunction.name
    i: DataFunctionInterface = function.get_interface()
    assert not i.inputs
    assert i.uses_context
    readme = mffunction.load_readme()
    assert len(readme) > 5
    assert len(mffunction.tests) == 2
    assert len(mffunction.tests[0]["inputs"]) == 1


def test_from_function():
    @datafunction
    def function1(ctx: DataFunctionContext):
        pass

    mffunction = DataFunctionPackage.from_function(function1)
    assert mffunction.root_path == Path(__file__).resolve()
    assert function1.name == mffunction.name
    function = mffunction.function
    assert function.name == "function1"
    i: DataFunctionInterface = function.get_interface()
    assert not i.inputs
    assert i.uses_context


def test_sql_function():
    pth = (
        Path(__file__).resolve().parents[0] / "_test_module/functions/test_sql_function"
    )
    mffunction = DataFunctionPackage.from_path(str(pth))
    function = mffunction.function
    assert function.name == mffunction.name
    i: DataFunctionInterface = function.get_interface()
    assert len(i.inputs) == 1
    assert i.uses_context
    readme = mffunction.load_readme()
    assert len(readme) > 5


# def test_from_module():
#     from . import _test_module

# functions = DataFunctionPackage.all_from_root_module(_test_module)
# print(functions)
# assert len(functions) == 2
