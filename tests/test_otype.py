from __future__ import annotations

from dataclasses import asdict

import pytest

from dags.core.typing.inference import (
    infer_otype_fields_from_records,
    infer_otype_from_records_list,
)
from dags.core.typing.object_type import (
    GeneratedObjectType,
    ObjectType,
    create_quick_otype,
    is_generic,
    otype_from_yaml,
)
from dags.modules import core
from tests.utils import make_test_env, sample_records

test_type_yml = """
name: TestType
version: 3
description: Description
unique on: uniq
on conflict: UpdateNullValues
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
    assert t1.name == "T1"
    assert t1.key == "T1"

    t2 = create_quick_otype(
        "TestType", fields=[("f1", "Unicode"), ("f2", "Integer")], module_key="m1"
    )
    assert t2.name == "TestType"
    assert t2.key == "m1.TestType"
    assert t2.get_identifier() == "m1_test_type"
    assert t2.get_field("f1").name == "f1"
    with pytest.raises(NameError):
        assert t2.get_field("f3")


def test_otype_helpers():
    assert is_generic("T")
    assert is_generic("Z")
    assert not is_generic("ZZ")
    assert not is_generic("11")


def test_otype_yaml():
    tt = otype_from_yaml(test_type_yml)
    assert tt.name == "TestType"
    assert tt.version == 3
    assert len(tt.fields) == 2
    assert len(tt.relationships) == 1
    assert len(tt.implementations) == 1


def test_otype_inference():
    fields = infer_otype_fields_from_records(sample_records)
    assert len(fields) == 9
    assert set(f.name for f in fields) == set("abcdefghi")
    field_types = {f.name: f.field_type for f in fields}
    assert field_types["a"] == "DateTime"
    assert (
        field_types["b"] == "Unicode"
    )  # TODO: what do we want this to be? Probably Unicode, SQL can't handle invalid date
    assert field_types["c"] == "BigInteger"
    assert field_types["d"] == "JSON"
    assert field_types["e"] == "JSON"
    assert field_types["f"] == "Unicode"
    assert field_types["g"] == "BigInteger"
    assert field_types["h"] == "UnicodeText"
    assert field_types["i"] == "UnicodeText"


def test_generated_otype():
    new_otype = infer_otype_from_records_list(sample_records)
    got = GeneratedObjectType(key=new_otype.key, definition=asdict(new_otype))
    env = make_test_env()
    with env.session_scope() as sess:
        sess.add(got)
    with env.session_scope() as sess:
        got = (
            sess.query(GeneratedObjectType)
            .filter(GeneratedObjectType.key == new_otype.key)
            .first()
        )
        got_type = got.as_otype()
        assert asdict(got_type) == asdict(new_otype)
    assert env.get_generated_otype(new_otype.key).key == new_otype.key
    assert env.get_generated_otype("pizza") is None


def test_any_otype():
    env = make_test_env()
    env.add_module(core)
    anytype = env.get_otype("Any")
    assert anytype.fields == []
