from __future__ import annotations

from pathlib import Path

from basis import Context, datafunction
from basis.core.declarative.function import (
    FunctionInterfaceCfg,
    FunctionSourceFileCfg,
)
from basis.core.function_package import (
    FunctionPackage,
    load_function_from_source_file,
)
from loguru import logger

logger.enable("basis")


def test_from_path():
    pth = Path(__file__).resolve().parents[0] / "_test_module/functions/test_function"
    mffunction = FunctionPackage.from_path(str(pth))
    function = mffunction.function
    assert function.name == mffunction.name
    i: FunctionInterfaceCfg = function.get_interface()
    assert not i.inputs
    assert i.uses_context
    readme = mffunction.load_readme()
    assert len(readme) > 5
    assert len(mffunction.tests) == 2
    assert len(mffunction.tests["test_1"]["inputs"]) == 1


def test_from_function():
    @datafunction
    def function1(ctx: Context):
        pass

    mffunction = FunctionPackage.from_function(function1)
    assert mffunction.root_path == Path(__file__).resolve()
    assert function1.name == mffunction.name
    function = mffunction.function
    assert function.name == "function1"
    i: FunctionInterfaceCfg = function.get_interface()
    assert not i.inputs
    assert i.uses_context


def test_sql_function():
    pth = (
        Path(__file__).resolve().parents[0] / "_test_module/functions/test_sql_function"
    )
    mffunction = FunctionPackage.from_path(str(pth))
    function = mffunction.function
    assert function.name == mffunction.name
    i: FunctionInterfaceCfg = function.get_interface()
    assert len(i.inputs) == 1
    assert i.uses_context
    readme = mffunction.load_readme()
    assert len(readme) > 5


def test_load_from_source_sql():
    src = FunctionSourceFileCfg(
        name="fn1", namespace="ns1", source="select * from t1", source_language="sql"
    )
    fn = load_function_from_source_file(src)
    assert fn.name == src.name


def test_load_from_source_py():
    src = FunctionSourceFileCfg(
        name="fn1",
        namespace="ns1",
        source="""
from basis import datafunction

@datafunction
def fn1():
    pass
        """,
        source_language="python",
    )
    fn = load_function_from_source_file(src)
    assert fn.name == src.name
