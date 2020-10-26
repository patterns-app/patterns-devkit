from __future__ import annotations

from dataclasses import asdict

import pytest

from dags.core.module import DEFAULT_LOCAL_MODULE_NAME
from dags.core.typing.inference import (
    infer_schema_fields_from_records,
    infer_schema_from_records_list,
)
from dags.core.typing.object_schema import (
    GeneratedObjectSchema,
    ObjectSchema,
    create_quick_schema,
    is_generic,
    schema_from_yaml,
)
from dags.modules import core
from tests.utils import make_test_env, sample_records

test_schema_yml = """
name: TestSchema
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
    schema: OtherSchema
    fields:
      other_field: other_field
implementations:
  SubType:
    sub_uniq: uniq
"""


def test_schema_identifiers():
    t1 = create_quick_schema("T1", fields=[("f1", "Unicode"), ("f2", "Integer")])
    assert t1.name == "T1"
    assert t1.key == f"{DEFAULT_LOCAL_MODULE_NAME}.T1"

    t2 = create_quick_schema(
        "TestSchema", fields=[("f1", "Unicode"), ("f2", "Integer")], module_name="m1"
    )
    assert t2.name == "TestSchema"
    assert t2.key == "m1.TestSchema"
    assert t2.get_identifier() == "m1_test_schema"
    assert t2.get_field("f1").name == "f1"
    with pytest.raises(NameError):
        assert t2.get_field("f3")


def test_schema_helpers():
    assert is_generic("T")
    assert is_generic("Z")
    assert not is_generic("ZZ")
    assert not is_generic("11")


def test_schema_yaml():
    tt = schema_from_yaml(test_schema_yml)
    assert tt.name == "TestSchema"
    assert tt.version == 3
    assert len(tt.fields) == 2
    assert len(tt.relationships) == 1
    assert len(tt.implementations) == 1


def test_schema_inference():
    fields = infer_schema_fields_from_records(sample_records)
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


def test_generated_schema():
    new_schema = infer_schema_from_records_list(sample_records)
    got = GeneratedObjectSchema(key=new_schema.key, definition=asdict(new_schema))
    env = make_test_env()
    with env.session_scope() as sess:
        sess.add(got)
    with env.session_scope() as sess:
        got = (
            sess.query(GeneratedObjectSchema)
            .filter(GeneratedObjectSchema.key == new_schema.key)
            .first()
        )
        got_schema = got.as_schema()
        assert asdict(got_schema) == asdict(new_schema)
    assert env.get_generated_schema(new_schema.key).key == new_schema.key
    assert env.get_generated_schema("pizza") is None


def test_any_schema():
    env = make_test_env()
    env.add_module(core)
    anyschema = env.get_schema("Any")
    assert anyschema.fields == []
