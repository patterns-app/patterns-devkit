from __future__ import annotations

import decimal
from dataclasses import asdict
from datetime import date, datetime
from sys import implementation

import pandas as pd
import pytest
from snapflow.core.module import DEFAULT_LOCAL_MODULE_NAME
from snapflow.core.snap_interface import get_schema_translation
from snapflow.core.typing.inference import infer_schema_from_records
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
from snapflow.schema.casting import (
    CastToSchemaLevel,
    SchemaTypeError,
    cast_to_realized_schema,
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
    type: Text(3)
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
    t1 = create_quick_schema("T1", fields=[("f1", "Text"), ("f2", "Integer")])
    assert t1.name == "T1"
    assert t1.key == f"{DEFAULT_LOCAL_MODULE_NAME}.T1"

    t2 = create_quick_schema(
        "TestSchema", fields=[("f1", "Text"), ("f2", "Integer")], module_name="m1"
    )
    assert t2.name == "TestSchema"
    assert t2.key == "m1.TestSchema"
    assert t2.get_identifier() == "m_1_test_schema"
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
    t_base = create_quick_schema("t_base", fields=[("f1", "Text"), ("f2", "Integer")])
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


ERROR = "_error"
WARN = "_warn"


@pytest.mark.parametrize(
    "cast_level,inferred,nominal,expected",
    [
        # Exact match
        [
            CastToSchemaLevel.SOFT,
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer")],
        ],
        [
            CastToSchemaLevel.HARD,
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer")],
        ],
        # Inferred has extra field
        [
            CastToSchemaLevel.SOFT,
            [("f1", "Text"), ("f2", "Integer"), ("f3", "Integer")],
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer"), ("f3", "Integer")],
        ],
        [
            CastToSchemaLevel.HARD,
            [("f1", "Text"), ("f2", "Integer"), ("f3", "Integer")],
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer")],
        ],
        # Nominal has extra field
        [
            CastToSchemaLevel.SOFT,
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer"), ("f3", "Integer")],
            [("f1", "Text"), ("f2", "Integer"), ("f3", "Integer")],
        ],
        [
            CastToSchemaLevel.HARD,
            [("f1", "Text"), ("f2", "Integer")],
            [("f1", "Text"), ("f2", "Integer"), ("f3", "Integer")],
            ERROR,
        ],
        # Inferred field mismatch WARN
        [
            CastToSchemaLevel.SOFT,
            [("f1", "Text"), ("f2", "Text")],
            [("f1", "Text"), ("f2", "LongText")],
            WARN,
        ],
        [
            CastToSchemaLevel.HARD,
            [("f1", "Text"), ("f2", "Text")],
            [("f1", "Text"), ("f2", "LongText")],
            WARN,  # We only warn now
        ],
        # Inferred field mismatch FAIL
        [
            CastToSchemaLevel.SOFT,
            [("f1", "Text"), ("f2", "Text")],
            [("f1", "Text"), ("f2", "Integer")],
            WARN,  # We only warn now
        ],
        [
            CastToSchemaLevel.HARD,
            [("f1", "Text"), ("f2", "Text")],
            [("f1", "Text"), ("f2", "Integer")],
            WARN,  # We only warn now
            # ERROR,
        ],
    ],
)
def test_cast_to_schema(cast_level, inferred, nominal, expected):
    inferred = create_quick_schema("Inf", fields=inferred)
    nominal = create_quick_schema("Nom", fields=nominal)
    if expected not in (ERROR, WARN):
        expected = create_quick_schema("Exp", fields=expected)
    env = make_test_env()
    with env.session_scope() as sess:
        if expected == ERROR:
            with pytest.raises(SchemaTypeError):
                s = cast_to_realized_schema(env, sess, inferred, nominal, cast_level)
        elif expected == WARN:
            with pytest.warns(UserWarning):
                s = cast_to_realized_schema(env, sess, inferred, nominal, cast_level)
        else:
            s = cast_to_realized_schema(env, sess, inferred, nominal, cast_level)
            for f in s.fields:
                e = expected.get_field(f.name)
                assert f == e
