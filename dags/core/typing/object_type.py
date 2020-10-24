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


ObjectTypeKey = str
ObjectTypeName = str


# Too much, just use str
# @dataclass(frozen=True)
# class ObjectTypeName:
#     name: str
#     module: Optional[str] = None
#
#     @classmethod
#     def from_str(cls, k: str) -> ObjectTypeName:
#         return ObjectTypeName(*k.split(".")[::-1])


@dataclass(frozen=True)
class Relationship:
    name: str
    type: ObjectTypeKey
    fields: Dict[str, str]


@dataclass(frozen=True)
class Implementation:
    type: ObjectTypeKey
    fields: Dict[str, str]


# TODO: support these?
class ConflictBehavior(StringEnum):
    ReplaceWithNewer = "ReplaceWithNewer"
    UpdateNullValues = "UpdateNullValues"
    UpdateWithNewerNotNullValues = "UpdateWithNewerNotNullValues"
    CreateNewVersion = (
        "CreateNewVersion"  # TODO: would need a field ("_record_version") or something
    )


class ObjectTypeClass(StringEnum):
    Entity = "Entity"
    Observation = "Observation"


def otype_key_to_identifier(key: str) -> str:
    key = key.replace(".", "_")
    return title_to_snake_case(key)


@dataclass(frozen=True)
class ObjectType:
    name: str
    module_key: Optional[str]
    version: Optional[str]
    # type_class: ObjectTypeClass
    description: str
    unique_on: List[str]
    on_conflict: ConflictBehavior
    fields: List[Field]
    relationships: List[Relationship] = field(default_factory=list)
    implementations: List[ObjectTypeKey] = field(default_factory=list)
    extends: Optional[
        ObjectTypeKey
    ] = None  # TODO: TBD how useful this would be, or exactly how it would work
    raw_definition: Optional[str] = None
    updated_at_field_name: Optional[str] = None  # TODO: TBD if we want this
    # parameterized_by: Sequence[str] = field(default_factory=list) # This is like GPV use case in CountryIndicator
    # parametererized_from: Optional[ObjectTypeName] = None
    # unregistered: bool = False
    # Data set order of magnitude / expected cardinality ? 1e2, country = 1e2, EcommOrder = 1e6 (Optional for sure, and overridable as "compiler hint")
    #       How often is this a property of the type, and how often a property of the SourceResource? SR probably better place for it
    # ckeyng window? data records are supposed to be stateless (are they?? what about Product name), but often not possible. Ckeyng window sets the duration of statefulness for a record
    # late arriving?
    # statefulness?

    @property
    def key(self) -> str:
        k = self.name
        if self.module_key:
            k = self.module_key + "." + k
        return k

    def get_identifier(self) -> str:  # TODO: better name for this fn
        return otype_key_to_identifier(self.key)

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
    def from_dict(cls, d: Dict) -> ObjectType:
        oc = d["on_conflict"]
        if isinstance(oc, str):
            d["on_conflict"] = ConflictBehavior(oc)
        fields = []
        for f in d["fields"]:
            fields.append(build_field_type_from_dict(f))
        d["fields"] = fields
        return ObjectType(**d)


ObjectTypeLike = Union[ObjectType, ObjectTypeKey, ObjectTypeName]


class GeneratedObjectType(BaseModel):
    key = Column(String, primary_key=True)
    definition = Column(JSON)

    def __repr__(self):
        return self._repr(name=self.key)

    def as_otype(self) -> ObjectType:
        assert isinstance(self.definition, dict)
        return ObjectType.from_dict(self.definition)


def is_generic(otype_like: ObjectTypeLike) -> bool:
    name = otype_like_to_name(otype_like)
    return len(name) == 1


def is_any(otype_like: ObjectTypeLike) -> bool:
    name = otype_like_to_name(otype_like)
    return name == "Any"


def otype_like_to_name(d: ObjectTypeLike) -> str:
    if isinstance(d, ObjectType):
        return d.name
    if isinstance(d, str):
        return d.split(".")[-1]
    raise TypeError(d)


def otype_from_yaml(yml: str, **overrides: Any) -> ObjectType:
    # TODO: add strictyaml schema
    d = strictyaml.load(yml).data
    d = clean_raw_otype_defintion(d)
    return build_otype_from_dict(d, **overrides)


def clean_keys(d: dict) -> dict:
    return {k.lower().replace(" ", "_"): v for k, v in d.items()}


def clean_raw_otype_defintion(raw_def: dict) -> dict:
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
    if "module_key" not in raw_def:
        raw_def["module_key"] = raw_def.pop("module", None)
    raw_def["version"] = int(raw_def["version"])
    return raw_def


def build_otype_from_dict(d: dict, **overrides: Any) -> ObjectType:
    fields = [build_field_type_from_dict(f) for f in d.pop("fields", [])]
    d["fields"] = fields
    d.update(**overrides)
    otype = ObjectType(**d)
    return otype


def build_field_type_from_dict(d: dict) -> Field:
    d["validators"] = [load_validator_from_dict(f) for f in d.pop("validators", [])]
    ftype = Field(**d)
    return ftype


def load_validator_from_dict(v: str) -> Validator:
    # TODO
    return Validator()


def otype_to_yaml(otype: ObjectType) -> str:
    if otype.raw_definition:
        return otype.raw_definition
    yml = f"name: {otype.name}\nversion: {otype.version}\ndescription: {otype.description}\n"
    unique_list = "\n  - ".join(otype.unique_on)
    yml += f"unique on: \n  - {unique_list}\n"
    yml += f"on conflict: {otype.on_conflict}\nfields:\n"
    for f in otype.fields:
        y = field_to_yaml(f)
        yml += f"  {y}\n"
    return yml

    # TODO:
    # on_conflict: ConflictBehavior
    # relationships: List[Reference] = field(default_factory=list)
    # implementations: List[ObjectTypeName] = field(default_factory=list)
    # extends: Optional[ObjectTypeName] = None


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
def create_quick_otype(name: str, fields: List[Tuple[str, str]], **kwargs):
    defaults: Dict[str, Any] = dict(
        name=name,
        module_key=None,
        # type_class="Observation",
        version="1.0",
        description="...",
        unique_on=[],
        implementations=[],
        on_conflict="ReplaceWithNewer",
    )
    defaults.update(kwargs)
    defaults["fields"] = [create_quick_field(f[0], f[1]) for f in fields]
    otype = ObjectType(**defaults)  # type: ignore
    return otype
