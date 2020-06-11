from __future__ import annotations

from basis.core.module import BasisModule


def test_module_init():
    from . import _test_module

    assert isinstance(_test_module, BasisModule)
    # Otypes
    assert len(_test_module.otypes) == 1
    testtype = list(_test_module.otypes)[0]
    assert testtype.name == "TestType"
    assert testtype.module_name == "_test_module"
    # Functions
    assert len(_test_module.functions) == 1
    sql_df = list(_test_module.functions)[0]
    assert sql_df.name == "test_sql"
    assert sql_df.module_name == "_test_module"
    # External
    assert len(_test_module.external_resources) == 1
    r = list(_test_module.external_resources)[0]
    assert r.name == "TestExtResource"
    assert r.module_name == "_test_module"
