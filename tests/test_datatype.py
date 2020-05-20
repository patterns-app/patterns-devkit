from __future__ import annotations

import pytest

from basis.core.object_type import (
    DEFAULT_MODULE_KEY,
    create_quick_otype,
    is_generic,
    is_uri,
    otype_from_yaml,
)


def test_otype_identifiers():
    t1 = create_quick_otype("T1", fields=[("f1", "Unicode"), ("f2", "Integer")])
    assert t1.key == "T1"
    assert t1.uri == DEFAULT_MODULE_KEY + ".T1"

    t2 = create_quick_otype(
        "TestType", fields=[("f1", "Unicode"), ("f2", "Integer")], module_key="m1"
    )
    assert t2.key == "TestType"
    assert t2.uri == "m1.TestType"
    assert t2.get_identifier() == "m1_test_type"
    assert t2.get_field("f1").name == "f1"
    with pytest.raises(NameError):
        assert t2.get_field("f3")


def test_otype_helpers():
    assert is_uri("t.two")
    assert is_uri("a.1")
    assert not is_uri("1")
    assert not is_uri("a")
    assert not is_uri("two")
    assert is_generic("T")
    assert is_generic("Z")
    assert not is_generic("ZZ")
    assert not is_generic("11")


def test_otype_yaml():
    test_type_yml = """
    key: TestType
    version: 3
    class: Entity
    description: Description
    unique on: uniq
    on conflict: MergeUpdateNullValues
    fields:
      uniq:
        type: Unicode(3)
        validators:
          - NotNull
      other_field:
        type: Integer
    relationships:
      other:
        type: OtherType
        fields:
          other_field: other_field
    implementations:
      SubType:
        sub_uniq: uniq
    """

    tt = otype_from_yaml(test_type_yml)
    assert tt.key == "TestType"
    assert tt.version == 3
    assert len(tt.fields) == 2
    assert len(tt.relationships) == 1
    assert len(tt.implementations) == 1
