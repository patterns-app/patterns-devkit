from __future__ import annotations

import pytest

from basis.core.registries import ObjectTypeRegistry
from basis.core.typing.inference import infer_otype_fields_from_records
from basis.core.typing.object_type import (
    create_quick_otype,
    is_generic,
    otype_from_yaml,
)
from basis.utils.uri import DEFAULT_MODULE_KEY, is_uri

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
    tt = otype_from_yaml(test_type_yml)
    assert tt.key == "TestType"
    assert tt.version == 3
    assert len(tt.fields) == 2
    assert len(tt.relationships) == 1
    assert len(tt.implementations) == 1


sample_records = [
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "1/1/2020",
        "c": "2020",
        "d": [1, 2, 3],
        "e": {1: 2},
        "f": "1.3",
        "g": 123,
    },
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "1/1/2020",
        "c": "12",
        "d": [1, 2, 3],
        "e": {1: 2},
        "f": "cookies",
        "g": 123,
    },
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "30/30/2020",
        "c": "12345",
        "d": [1, 2, 3],
        "e": "string",
        "f": "true",
        "g": 12345,
    },
    {"a": None, "b": None, "c": None, "d": None, "e": None, "f": None, "g": None},
]


def test_otype_inference():
    fields = infer_otype_fields_from_records(sample_records)
    assert len(fields) == 7
    assert set(f.name for f in fields) == set("abcdefg")
    field_types = {f.name: f.field_type for f in fields}
    assert field_types["a"] == "DateTime"
    assert field_types["b"] == "UnicodeText"
    assert field_types["c"] == "UnicodeText"
    assert field_types["d"] == "JSON"
    assert field_types["e"] == "JSON"
    assert field_types["f"] == "UnicodeText"
    # assert field_types["g"] == "BigInteger"  # TODO: Fix this. See notes on type inference and why pandas not sufficient


def test_otype_registry():
    r = ObjectTypeRegistry()
    t1 = create_quick_otype("T1", fields=[("f1", "Unicode"), ("f2", "Integer")])
    t2 = create_quick_otype(
        "TestType", fields=[("f1", "Unicode"), ("f2", "Integer")], module_key="m1"
    )
    r.process_and_register_all([t1, t2])
    assert r.get("T1") is t1
    assert r.get("m1.TestType") is t2
