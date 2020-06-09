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
    # Providers
    assert len(_test_module.providers) == 1
    p = list(_test_module.providers)[0]
    assert p.name == "test_provider"
    assert p.module_name == "_test_module"
    assert len(p.resources) == 1
