from __future__ import annotations

import warnings
from dataclasses import asdict
from enum import Enum
from functools import total_ordering
from typing import TYPE_CHECKING, Any, Iterable

from commonmodel import Field, FieldType, Schema

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


def is_strict_field_match(f1: Field, f2: Field) -> bool:
    """
    Fields match strictly if names match and field types are exact match (including all parameters)
    """
    return f1.name == f2.name and f1.field_type == f2.field_type


def is_strict_schema_match(schema1: Schema, schema2: Schema) -> bool:
    """
    Schemas match strictly if all fields match strictly one-to-one
    """
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
    # TODO inferred field will not have any validators
    supr_fields = [f.name for f in supr.fields]  # if not f.is_nullable()]
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


def check_casts(
    from_schema: Schema,
    to_schema: Schema,
    cast_level: CastToSchemaLevel = CastToSchemaLevel.SOFT,
    warn_on_downcast=True,
    fail_on_downcast=False,
):
    """
    TODO: This is ok, but not really matching the reality of casting.
    Casting is really a separate ETL step: it's runtime / storage
    specific, has many different plausible approaches, and is too
    complex to be managed at one level with no context (like we don't
    even know what runtime we are on here)
    Best option for now is to give the user information on what is happening
    (via warns and logging) but not actually block anything --
    let errors arise naturally as they will, otherwise "play on".
    """
    for f in from_schema.fields:
        try:
            new_f = to_schema.get_field(f.name)
            if not is_strict_field_match(f, new_f):
                # if cast_level == CastToSchemaLevel.HARD:
                #     raise SchemaTypeError(
                #         f"Cannot cast (Cast level == HARD): {f.field_type} to {new_f.field_type} "
                #     )
                # if not f.field_type.is_castable_to_type(new_f.field_type):
                #     raise SchemaTypeError(
                #         f"Cannot cast: {f.field_type} to {new_f.field_type}"
                #     )
                if warn_on_downcast:
                    warnings.warn(
                        f"Downcasting field '{f.name}': {f.field_type} to {new_f.field_type}"
                    )
                if fail_on_downcast:
                    raise SchemaTypeError(
                        f"Cannot cast (FAIL_ON_DOWNCAST=True) '{f.name}': {f.field_type} to {new_f.field_type} "
                    )
        except NameError:
            pass


def cast_to_realized_schema(
    env: Environment,
    inferred_schema: Schema,
    nominal_schema: Schema,
    cast_level: CastToSchemaLevel = CastToSchemaLevel.SOFT,
) -> Schema:
    realized = None
    if cast_level == CastToSchemaLevel.NONE:
        realized = inferred_schema
    else:
        if is_strict_schema_match(inferred_schema, nominal_schema):
            realized = nominal_schema
        elif has_subset_fields(nominal_schema, inferred_schema):
            if cast_level < CastToSchemaLevel.HARD:
                realized = update_matching_field_definitions(
                    env, inferred_schema, nominal_schema
                )
            else:
                realized = nominal_schema
        elif has_subset_nonnull_fields(nominal_schema, inferred_schema):
            if cast_level < CastToSchemaLevel.HARD:
                realized = update_matching_field_definitions(
                    env, inferred_schema, nominal_schema
                )
            else:
                pass
    if realized is None:
        raise SchemaTypeError(
            f"Inferred schema does not have necessary columns and cast level set to {cast_level}. "
            f"Inferred columns: {inferred_schema.field_names()}, "
            f"nominal columns: {nominal_schema.field_names()} "
        )
    if realized != inferred_schema:
        check_casts(
            inferred_schema,
            realized,
            cast_level=cast_level,
            warn_on_downcast=env.settings.warn_on_downcast,
            fail_on_downcast=env.settings.fail_on_downcast,
        )
    return realized
