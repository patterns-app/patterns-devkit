from __future__ import annotations

import decimal
from dataclasses import asdict
from datetime import date, datetime
from sys import implementation

import pandas as pd
import pytest
from snapflow.core.module import DEFAULT_LOCAL_MODULE_NAME
from snapflow.core.snap_interface import get_schema_translation
from snapflow.core.typing.inference import (
    cast_python_object_to_sqlalchemy_type,
    infer_schema_fields_from_records,
    infer_schema_from_records,
)
from snapflow.modules import core
from snapflow.schema.base import (
    DEFAULT_UNICODE_TEXT_TYPE,
    DEFAULT_UNICODE_TYPE,
    GeneratedSchema,
    Implementation,
    Schema,
    create_quick_schema,
    is_generic,
    schema_from_yaml,
)
from tests.utils import make_test_env, sample_records

test_schema_yml = """
name: TestSchema
version: 3
description: Description
unique_on: uniq
immutable: false
fields:
  uniq:
    type: Unicode(3)
    validators:
      - NotNull
  other_field:
    type: Integer
relations:
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
    assert tt.version == "3"
    assert len(tt.fields) == 2
    assert len(tt.relations) == 1
    assert len(tt.implementations) == 1
    assert tt.implementations[0].schema_key == "SubType"
    assert tt.implementations[0].fields == {"sub_uniq": "uniq"}


def test_schema_translation():
    env = make_test_env()
    t_base = create_quick_schema(
        "t_base", fields=[("f1", "Unicode"), ("f2", "Integer")]
    )
    t_impl = create_quick_schema(
        "t_impl",
        fields=[("g1", "Unicode"), ("g2", "Integer")],
        implementations=[Implementation("t_base", {"f1": "g1", "f2": "g2"})],
    )
    env.add_schema(t_base)
    env.add_schema(t_impl)
    with env.session_scope() as sess:
        trans = get_schema_translation(
            env, sess, source_schema=t_impl, target_schema=t_base
        )
        assert trans.translation == {"g1": "f1", "g2": "f2"}


def test_schema_inference():
    fields = infer_schema_fields_from_records(sample_records)
    assert len(fields) == 9
    assert set(f.name for f in fields) == set("abcdefghi")
    field_types = {f.name: f.field_type for f in fields}
    assert field_types["a"] == "DateTime"
    assert field_types["b"] == DEFAULT_UNICODE_TYPE  # Invalid date, so is unicode
    assert field_types["c"] == "BigInteger"
    assert field_types["d"] == "JSON"
    assert field_types["e"] == DEFAULT_UNICODE_TYPE
    assert field_types["f"] == DEFAULT_UNICODE_TYPE
    assert field_types["g"] == "BigInteger"
    assert field_types["h"] == DEFAULT_UNICODE_TEXT_TYPE
    assert field_types["i"] == DEFAULT_UNICODE_TYPE


def test_generated_schema():
    new_schema = infer_schema_from_records(sample_records)
    got = GeneratedSchema(key=new_schema.key, definition=asdict(new_schema))
    env = make_test_env()
    with env.session_scope() as sess:
        sess.add(got)
        got = (
            sess.query(GeneratedSchema)
            .filter(GeneratedSchema.key == new_schema.key)
            .first()
        )
        got_schema = got.as_schema()
        assert asdict(got_schema) == asdict(new_schema)
        assert env.get_generated_schema(new_schema.key, sess).key == new_schema.key
        assert env.get_generated_schema("pizza", sess) is None


def test_any_schema():
    env = make_test_env()
    env.add_module(core)
    with env.session_scope() as sess:
        anyschema = env.get_schema("Any", sess)
    assert anyschema.fields == []


ERROR = "_ERROR"


@pytest.mark.parametrize(
    "satype,obj,expected",
    [
        ("Integer", 1, 1),
        ("Integer", "1", 1),
        ("Integer", "01", 1),
        ("Integer", None, None),
        ("Integer", pd.NA, None),
        ("DateTime", "2020-01-01", datetime(2020, 1, 1)),
        ("DateTime", "2020-01-01 00:00:00", datetime(2020, 1, 1)),
        ("DateTime", 1577836800, datetime(2020, 1, 1)),
        ("JSON", [1, 2], [1, 2]),
        ("JSON", {"a": 2}, {"a": 2}),
        ("JSON", '{"1":2}', {"1": 2}),
        ("JSON", None, None),
        ("Boolean", "true", True),
        ("Boolean", "false", False),
        ("Boolean", "t", True),
        ("Boolean", "f", False),
        ("Boolean", 1, True),
        ("Boolean", 0, False),
        ("Boolean", "Hi", ERROR),
        ("Boolean", "Hi", None),  # If we ignore the error, it should be null
        ("Date", "2020-01-01", date(2020, 1, 1)),
        ("Date", "2020-01-01 00:00:00", date(2020, 1, 1)),
        ("Date", "Hi", ERROR),
        ("Unicode", "Hi", "Hi"),
        ("Unicode", 0, "0"),
        ("Unicode", None, None),
    ],
)
def test_value_casting(satype, obj, expected):
    if expected == ERROR:
        with pytest.raises(Exception):
            cast_python_object_to_sqlalchemy_type(
                obj, satype, ignore_error_as_null=False
            )
    else:
        assert cast_python_object_to_sqlalchemy_type(obj, satype) == expected


def test_schema_casting():
    # TODO
    pass
