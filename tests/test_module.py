from __future__ import annotations

from dags.core.module import DagsModule
from dags.modules import core


def test_module_init():
    from . import _test_module

    assert isinstance(_test_module, DagsModule)
    # Otypes
    assert len(_test_module.otypes) == 1
    testtype = list(_test_module.otypes)[0]
    assert testtype.name == "TestType"
    assert testtype.module_key == "_test_module"
    # Pipes
    assert len(_test_module.pipes) == 2
    assert set(f.key for f in _test_module.pipes) == {"_test_module.test_sql", "df1"}


def test_core_modules():
    core.run_tests()  # TODO: This is not a unit test, should separate out
