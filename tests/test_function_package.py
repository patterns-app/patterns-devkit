from __future__ import annotations

import logging
from pprint import pprint
from pathlib import Path
from snapflow.core.execution.execution import FunctionContext
from snapflow.core.function_interface import DeclaredFunctionInterface
from loguru import logger
from snapflow.core.environment import Environment
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.core.function_package import FunctionPackage
from snapflow.modules import core
from snapflow import _Function, Function

logger.enable("snapflow")


def test_from_path():
    pth = Path(__file__).resolve().parents[0] / "_test_module/functions/test_function"
    mffunction = FunctionPackage.from_path(str(pth))
    function = mffunction.function
    assert function.name == mffunction.name
    i: DeclaredFunctionInterface = function.get_interface()
    assert not i.inputs
    assert i.context is not None
    readme = mffunction.load_readme()
    assert len(readme) > 5
    assert len(mffunction.tests) == 2
    assert len(mffunction.tests[0]["inputs"]) == 1


def test_from_function():
    @Function
    def function1(ctx: FunctionContext):
        pass

    mffunction = FunctionPackage.from_function(function1)
    assert mffunction.root_path == Path(__file__).resolve()
    assert function1.name == mffunction.name
    function = mffunction.function
    assert function.name == "function1"
    i: DeclaredFunctionInterface = function.get_interface()
    assert not i.inputs
    assert i.context is not None


def test_sql_function():
    pth = (
        Path(__file__).resolve().parents[0] / "_test_module/functions/test_sql_function"
    )
    mffunction = FunctionPackage.from_path(str(pth))
    function = mffunction.function
    assert function.name == mffunction.name
    i: DeclaredFunctionInterface = function.get_interface()
    assert len(i.inputs) == 1
    assert i.context is not None
    readme = mffunction.load_readme()
    assert len(readme) > 5


# def test_from_module():
#     from . import _test_module

# functions = FunctionPackage.all_from_root_module(_test_module)
# print(functions)
# assert len(functions) == 2
