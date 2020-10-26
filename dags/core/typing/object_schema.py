from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import strictyaml
from sqlalchemy import JSON, Column, String

from dags.core.metadata.orm import BaseModel
from dags.utils.common import StringEnum, title_to_snake_case
from loguru import logger

### Not needed atm, maybe in the future
# def parse_field_type(ft: str):
#     a = ast.parse("Reference(Column, iso_code, nullable=False)")
#     ast_call = a.body[0].value
#     tipe = ast_call.func.id
#     args = [a.id for a in ast_call.args]
#     kwargs = {k.arg: k.value.id for k in ast_call.args}
#     return
#
#
# @dataclass(frozen=True)
# class FieldType:
#     field_type_class: str
#     args: List[str]
#     kwargs: Dict[str, str]
#
#     @classmethod
#     def from_field_type_str(cls, ft_str: str) -> FieldType:
#         a = ast.parse(ft_str)
#         ast_call = a.body[0].value
#         tipe = ast_call.func.id
#         args = [a.id for a in ast_call.args]
#         kwargs = {k.arg: k.value.id for k in ast_call.args}
#         return FieldType(field_type_class=tipe, args=args, kwargs=kwargs,)


@dataclass(frozen=True)
class Field:
    name: str
    field_type: str
    validators: List[Validator] = field(default_factory=list)
    index: bool = False
    is_metadata: bool = False
    description: Optional[str] = None


class Validator:
    # TODO
    pass


ObjectSchemaKey = str
ObjectSchemaName = str


# Too much, just use str
# @dataclass(frozen=True)
# class ObjectSchemaName:
#     name: str
#     module: Optional[str] = None
#
#     @classmethod
#     def from_str(cls, k: str) -> ObjectSchemaName:
#         return ObjectSchemaName(*k.split(".")[::-1])


@dataclass(frozen=True)
class Relationship:
    name: str
    type: ObjectSchemaKey
    fields: Dict[str, str]


@dataclass(frozen=True)
class Implementation:
    type: ObjectSchemaKey
    fields: Dict[str, str]


# TODO: support these?
class ConflictBehavior(StringEnum):
    ReplaceWithNewer = "ReplaceWithNewer"
    UpdateNullValues = "UpdateNullValues"
    UpdateWithNewerNotNullValues = "UpdateWithNewerNotNullValues"
    CreateNewVersion = (
        "CreateNewVersion"  # TODO: would need a field ("_record_version") or something
    )


class ObjectSchemaClass(StringEnum):
    Entity = "Entity"
    Observation = "Observation"


def schema_key_to_identifier(key: str) -> str:
    key = key.replace(".", "_")
    return title_to_snake_case(key)


@dataclass(frozen=True)
class ObjectSchema:
    name: str
    module_name: Optional[str]
    version: Optional[str]
    description: str
    unique_on: List[str]
    on_conflict: ConflictBehavior
    fields: List[Field]
    relationships: List[Relationship] = field(default_factory=list)
    implementations: List[ObjectSchemaKey] = field(default_factory=list)
    raw_definition: Optional[str] = None
    extends: Optional[
        ObjectSchemaKey
    ] = None  # TODO: TBD how useful this would be, or exactly how it would work
    updated_at_field_name: Optional[str] = None  # TODO: TBD if we want this
    """
    Things that are a property of the SOURCE of data, not the Schema definition
        - Data set expected cardinality / order of magnitude ?
            This is actually sometimes a property of a the ObjectSchema (`Country` you could argue, for instance, less than 1000)
            Other times more likely to be property of source (`Customer` for instance, could be 100 or 100M)
        - Curing window
            Records are ideally stateless (think event logs), but many data sources produce stateful
            uniquely identified records (think ecomm transaction with cancelled field)
        - Late arriving
            Relatedly, some sources produce late-arriving records (significantly out-of-order records)
            Not a property of the Schema
    """

    @property
    def key(self) -> str:
        k = self.name
        if self.module_name:
            k = self.module_name + "." + k
        return k

    def get_identifier(self) -> str:  # TODO: better name for this fn
        return schema_key_to_identifier(self.key)

    def get_field(self, field_name: str) -> Field:
        for f in self.fields:
            if f.name == field_name:
                return f
        # TODO: relationships
        raise NameError

    @property
    def updated_at_field(self) -> Optional[Field]:
        if self.updated_at_field_name:
            return self.get_field(self.updated_at_field_name)
        try:
            return self.get_field("updated_at")
        except NameError:
            return None

    @classmethod
    def from_dict(cls, d: Dict) -> ObjectSchema:
        oc = d["on_conflict"]
        if isinstance(oc, str):
            d["on_conflict"] = ConflictBehavior(oc)
        fields = []
        for f in d["fields"]:
            fields.append(build_field_type_from_dict(f))
        d["fields"] = fields
        return ObjectSchema(**d)


ObjectSchemaLike = Union[ObjectSchema, ObjectSchemaKey, ObjectSchemaName]


class GeneratedObjectSchema(BaseModel):
    key = Column(String, primary_key=True)
    definition = Column(JSON)

    def __repr__(self):
        return self._repr(name=self.key)

    def as_schema(self) -> ObjectSchema:
        assert isinstance(self.definition, dict)
        return ObjectSchema.from_dict(self.definition)


def is_generic(schema_like: ObjectSchemaLike) -> bool:
    name = schema_like_to_name(schema_like)
    return len(name) == 1


def is_any(schema_like: ObjectSchemaLike) -> bool:
    name = schema_like_to_name(schema_like)
    return name == "Any"


def schema_like_to_name(d: ObjectSchemaLike) -> str:
    if isinstance(d, ObjectSchema):
        return d.name
    if isinstance(d, str):
        return d.split(".")[-1]
    raise TypeError(d)


def schema_from_yaml(yml: str, **overrides: Any) -> ObjectSchema:
    # TODO: add strictyaml schema
    d = strictyaml.load(yml).data
    d = clean_raw_schema_defintion(d)
    return build_schema_from_dict(d, **overrides)


def clean_keys(d: dict) -> dict:
    return {k.lower().replace(" ", "_"): v for k, v in d.items()}


def clean_raw_schema_defintion(raw_def: dict) -> dict:
    """
    """
    raw_def = clean_keys(raw_def)
    raw_fields = raw_def.pop("fields", {})
    raw_def["fields"] = []
    for name, f in raw_fields.items():
        nf = {"name": name, "field_type": f.pop("type", None)}
        nf.update(f)
        raw_def["fields"].append(nf)
    # TODO: validate unique_on fields are present in fields
    if "unique_on" not in raw_def:
        raw_def["unique_on"] = []
    if isinstance(raw_def.get("unique_on"), str):
        raw_def["unique_on"] = [raw_def["unique_on"]]
    oc = raw_def["on_conflict"]
    if isinstance(oc, str):
        raw_def["on_conflict"] = ConflictBehavior(oc)
    # raw_def["type_class"] = raw_def.pop("class", None)
    if "module_name" not in raw_def:
        raw_def["module_name"] = raw_def.pop("module", None)
    raw_def["version"] = int(raw_def["version"])
    return raw_def


def build_schema_from_dict(d: dict, **overrides: Any) -> ObjectSchema:
    fields = [build_field_type_from_dict(f) for f in d.pop("fields", [])]
    d["fields"] = fields
    d.update(**overrides)
    schema = ObjectSchema(**d)
    return schema


def build_field_type_from_dict(d: dict) -> Field:
    d["validators"] = [load_validator_from_dict(f) for f in d.pop("validators", [])]
    ftype = Field(**d)
    return ftype


def load_validator_from_dict(v: str) -> Validator:
    # TODO
    return Validator()


def schema_to_yaml(schema: ObjectSchema) -> str:
    if schema.raw_definition:
        return schema.raw_definition
    yml = f"name: {schema.name}\nversion: {schema.version}\ndescription: {schema.description}\n"
    unique_list = "\n  - ".join(schema.unique_on)
    yml += f"unique on: \n  - {unique_list}\n"
    yml += f"on conflict: {schema.on_conflict}\nfields:\n"
    for f in schema.fields:
        y = field_to_yaml(f)
        yml += f"  {y}\n"
    return yml

    # TODO:
    # on_conflict: ConflictBehavior
    # relationships: List[Reference] = field(default_factory=list)
    # implementations: List[ObjectSchemaName] = field(default_factory=list)
    # extends: Optional[ObjectSchemaName] = None


def field_to_yaml(f: Field) -> str:
    return f"{f.name}:\n    type: {f.field_type}"
    # TODO
    # validators
    # index
    # is_metadata
    # description


def create_quick_field(name: str, field_type: str, **kwargs) -> Field:
    args = dict(name=name, field_type=field_type, validators=[])
    args.update(kwargs)
    return Field(**args)  # type: ignore


# Helper
def create_quick_schema(name: str, fields: List[Tuple[str, str]], **kwargs):
    from dags.core.module import DEFAULT_LOCAL_MODULE_NAME

    defaults: Dict[str, Any] = dict(
        name=name,
        module_name=DEFAULT_LOCAL_MODULE_NAME,
        version="1.0",
        description="...",
        unique_on=[],
        implementations=[],
        on_conflict="ReplaceWithNewer",
    )
    defaults.update(kwargs)
    defaults["fields"] = [create_quick_field(f[0], f[1]) for f in fields]
    schema = ObjectSchema(**defaults)  # type: ignore
    return schema
