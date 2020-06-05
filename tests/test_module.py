from __future__ import annotations

import pytest

from basis.core.module import BasisModule
from basis.core.registries import ObjectTypeRegistry
from basis.core.typing.inference import infer_otype_fields_from_records
from basis.core.typing.object_type import (
    create_quick_otype,
    is_generic,
    otype_from_yaml,
)
from basis.utils.uri import DEFAULT_MODULE_KEY, is_uri


def test_module_init():
    from . import _test_module

    assert isinstance(_test_module, BasisModule)
    assert len(_test_module.otypes) == 1
    testtype = list(_test_module.otypes.all())[0]
    assert testtype.key == "TestType"
    assert testtype.module_key == "_test_module"
    assert len(_test_module.data_functions) == 1
    sql_df = list(_test_module.data_functions.all())[0]
    assert sql_df.key == "test_sql"
    assert sql_df.module_key == "_test_module"
