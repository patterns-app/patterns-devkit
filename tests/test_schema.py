from __future__ import annotations

import pytest
from commonmodel.base import create_quick_schema
from snapflow.core.typing.casting import (
    CastToSchemaLevel,
    SchemaTypeError,
    cast_to_realized_schema,
)
from tests.utils import make_test_env, sample_records

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
    with env.md_api.begin():
        if expected == ERROR:
            with pytest.raises(SchemaTypeError):
                s = cast_to_realized_schema(inferred, nominal, cast_level)
        elif expected == WARN:
            with pytest.warns(UserWarning):
                s = cast_to_realized_schema(inferred, nominal, cast_level)
        else:
            s = cast_to_realized_schema(inferred, nominal, cast_level)
            for f in s.fields:
                e = expected.get_field(f.name)
                assert f == e
