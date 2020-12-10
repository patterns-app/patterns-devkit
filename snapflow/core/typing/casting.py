from __future__ import annotations

from dataclasses import asdict
from enum import Enum
from functools import total_ordering
from typing import TYPE_CHECKING

from loguru import logger
from snapflow.core.typing.schema import (
    ConflictBehavior,
    Field,
    Schema,
    create_quick_field,
    create_quick_schema,
)

if TYPE_CHECKING:
    from snapflow import Environment


@total_ordering
class CastToSchemaLevel(Enum):
    NONE = 0
    SOFT = 1
    HARD = 2

    def __lt__(self, other):
        return self.value < other.value


class SchemaTypeError(Exception):
    pass


def is_same_fieldtype_class(ft1: str, ft2: str) -> bool:
    # TODO: a lot to unpack and do here:
    #   When do we accept fieldtypes as compatible? when do we "downcast"?
    #   BigInteger vs Integer, Unicode(256) vs Unicode(128), etc
    #   What are the settings that let a user control this?
    return ft1.split("(")[0] == ft2.split("(")[0]


def is_strict_field_match(f1: Field, f2: Field) -> bool:
    return is_same_fieldtype_class(f1.field_type, f2.field_type) and f1.name == f2.name


def is_strict_schema_match(schema1: Schema, schema2: Schema) -> bool:
    if set(schema1.field_names()) != set(schema2.field_names()):
        return False
    for f1 in schema1.fields:
        f2 = schema2.get_field(f1.name)
        if not is_strict_field_match(f1, f2):
            return False
    return True


def has_subset_fields(sub: Schema, supr: Schema) -> bool:
    return set(sub.field_names()) <= set(supr.field_names())


def has_subset_nonnull_fields(sub: Schema, supr: Schema) -> bool:
    sub_fields = [f.name for f in sub.fields if not f.is_nullable()]
    supr_fields = [f.name for f in supr.fields if not f.is_nullable()]
    return set(sub_fields) <= set(supr_fields)


def update_matching_field_definitions(
    env: Environment, schema: Schema, update_with_schema: Schema
) -> Schema:
    fields = []
    modified = False
    for f in schema.fields:
        new_f = f
        try:
            new_f = update_with_schema.get_field(f.name)
            modified = True
        except NameError:
            pass
        fields.append(new_f)
    if not modified:
        return schema
    schema_dict = asdict(schema)
    schema_dict["name"] = f"{schema.name}_with_{update_with_schema.name}"
    schema_dict["fields"] = fields
    updated = Schema.from_dict(schema_dict)
    env.add_new_generated_schema(updated)
    return updated


def cast_to_realized_schema(
    env: Environment,
    inferred_schema: Schema,
    nominal_schema: Schema,
    cast_level: CastToSchemaLevel = CastToSchemaLevel.SOFT,
):
    if is_strict_schema_match(inferred_schema, nominal_schema):
        return nominal_schema
    if has_subset_fields(nominal_schema, inferred_schema):
        if cast_level < CastToSchemaLevel.HARD:
            return update_matching_field_definitions(
                env, inferred_schema, nominal_schema
            )
        else:
            return nominal_schema
    elif has_subset_nonnull_fields(nominal_schema, inferred_schema):
        if cast_level < CastToSchemaLevel.HARD:
            return update_matching_field_definitions(
                env, inferred_schema, nominal_schema
            )
    if cast_level >= CastToSchemaLevel.NONE:
        raise SchemaTypeError(
            f"Inferred schema does not have necessary columns and cast level set to {cast_level}. "
            f"Inferred columns: {inferred_schema.field_names()}, "
            f"nominal columns: {nominal_schema.field_names()} "
        )
    return inferred_schema
