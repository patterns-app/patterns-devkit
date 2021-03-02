from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import strictyaml
from loguru import logger
from snapflow.core.metadata.orm import BaseModel
from snapflow.utils.common import StringEnum, ensure_bool, title_to_snake_case
from sqlalchemy import JSON, Column, String
from sqlalchemy.orm.session import Session

if TYPE_CHECKING:
    from snapflow import Environment


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

MAX_UNICODE_LENGTH = 255
DEFAULT_UNICODE_TYPE = f"Unicode({MAX_UNICODE_LENGTH})"
DEFAULT_UNICODE_TEXT_TYPE = "UnicodeText"


@dataclass(frozen=True)
class Field:
    name: str
    field_type: str
    validators: List[Validator] = field(default_factory=list)
    index: bool = False
    is_metadata: bool = False
    description: Optional[str] = None

    def is_nullable(self) -> bool:
        for v in self.validators:
            if "nonnull" in v.name.lower():
                return False
        return True


@dataclass(frozen=True)
class Validator:
    # TODO
    name: Optional[str] = None


SchemaKey = str
SchemaName = str


@dataclass(frozen=True)
class Relation:
    name: str
    schema: SchemaKey
    fields: Dict[str, str]


@dataclass(frozen=True)
class Implementation:
    schema_key: SchemaKey
    fields: Dict[str, str]

    def schema(self, env: Environment, sess: Session) -> Schema:
        return env.get_schema(self.schema_key, sess)

    def as_schema_translation(
        self, env: Environment, sess: Session, other: Schema
    ) -> SchemaTranslation:
        # TODO: this inversion is a bit confusing
        trans = {v: k for k, v in self.fields.items()}
        return SchemaTranslation(
            translation=trans, from_schema=self.schema(env, sess), to_schema=other
        )


def schema_key_to_identifier(key: str) -> str:
    key = key.replace(".", "_")
    key = title_to_snake_case(key)
    return re.sub("[_]+", "_", key)


@dataclass(frozen=True)
class Schema:
    name: str
    module_name: Optional[str]
    version: Optional[str]
    description: str
    unique_on: List[str]
    fields: List[Field]
    relations: List[Relation] = field(default_factory=list)
    implementations: List[Implementation] = field(default_factory=list)
    immutable: bool = False
    raw_definition: Optional[str] = None
    extends: Optional[
        SchemaKey
    ] = None  # TODO: TBD how useful this would be, or exactly how it would work
    updated_at_field_name: Optional[str] = None  # TODO: TBD if we want this
    """
    Things that are a property of the SOURCE of data, not the Schema definition
        - Data set expected cardinality / order of magnitude ?
            This is actually sometimes a property of a the Schema (`Country` you could argue, for instance, less than 1000)
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
        # TODO: relations
        raise NameError

    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]

    @property
    def updated_at_field(self) -> Optional[Field]:
        if self.updated_at_field_name:
            return self.get_field(self.updated_at_field_name)
        try:
            return self.get_field("updated_at")
        except NameError:
            return None

    @classmethod
    def from_dict(cls, d: Dict) -> Schema:
        fields = []
        for f in d["fields"]:
            if isinstance(f, dict):
                f = build_field_from_dict(f)
            elif isinstance(f, Field):
                pass
            else:
                raise ValueError(f)
            fields.append(f)
        d["fields"] = fields
        return Schema(**d)

    def get_translation_to(
        self, env: Environment, sess: Session, other: Schema
    ) -> Optional[SchemaTranslation]:
        if not self.implementations:
            return None
        for impl in self.implementations:
            schema = env.get_schema(impl.schema_key, sess)
            if schema.key == other.key:
                return impl.as_schema_translation(env, sess, other)
        return None


SchemaLike = Union[Schema, SchemaKey, SchemaName]


class SchemaTranslation:
    def __init__(
        self,
        translation: Optional[Dict[str, str]] = None,
        from_schema: Optional[Schema] = None,
        to_schema: Optional[Schema] = None,
    ):
        self.translation = translation
        self.from_schema = from_schema
        self.to_schema = to_schema

    def as_dict(self) -> Dict[str, str]:
        if not self.translation:
            raise NotImplementedError
        return self.translation


class GeneratedSchema(BaseModel):
    key = Column(String(128), primary_key=True)
    definition = Column(JSON)

    def __repr__(self):
        return self._repr(name=self.key)

    def as_schema(self) -> Schema:
        assert isinstance(self.definition, dict)
        return Schema.from_dict(self.definition)


def is_generic(schema_like: SchemaLike) -> bool:
    name = schema_like_to_name(schema_like)
    return len(name) == 1


def is_any(schema_like: SchemaLike) -> bool:
    name = schema_like_to_name(schema_like)
    return name == "Any"


def schema_like_to_name(d: SchemaLike) -> str:
    if isinstance(d, Schema):
        return d.name
    if isinstance(d, str):
        return d.split(".")[-1]
    raise TypeError(d)


def schema_from_yaml(yml: str, **overrides: Any) -> Schema:
    # TODO: add strictyaml schema
    d = strictyaml.load(yml).data
    d = clean_raw_schema_defintion(d)
    return build_schema_from_dict(d, **overrides)


def clean_keys(d: dict) -> dict:
    return {k.lower().replace(" ", "_"): v for k, v in d.items()}


def clean_raw_schema_defintion(raw_def: dict) -> dict:
    """"""
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
    ir = raw_def.get("immutable")
    if isinstance(ir, str):
        raw_def["immutable"] = ensure_bool(ir)
    # raw_def["type_class"] = raw_def.pop("class", None)
    if "module_name" not in raw_def:
        raw_def["module_name"] = raw_def.pop("module", None)
    return raw_def


def build_schema_from_dict(d: dict, **overrides: Any) -> Schema:
    fields = [build_field_from_dict(f) for f in d.pop("fields", [])]
    d["implementations"] = [
        Implementation(k, v) for k, v in d.pop("implementations", {}).items()
    ]
    # TODO: relations and implementations
    d["fields"] = fields
    d.update(**overrides)
    schema = Schema(**d)
    return schema


def build_field_from_dict(d: dict) -> Field:
    d["validators"] = [load_validator_from_dict(f) for f in d.pop("validators", [])]
    conform_field_type(d.get("field_type"))
    ftype = Field(**d)
    return ftype


def conform_field_type(ft: str) -> str:
    # TODO: this is hidden and only affects this code path... Where do we want to conform this?
    if ft is None or ft in ("Unicode", "UnicodeText"):
        return DEFAULT_UNICODE_TYPE
    return ft


def load_validator_from_dict(v: str) -> Validator:
    # TODO
    return Validator(v)


def schema_to_yaml(schema: Schema) -> str:
    # TODO: Very rough version, not for production use. Also, use existing tool for this?
    if schema.raw_definition:
        return schema.raw_definition
    yml = f"name: {schema.name}\nversion: {schema.version}\ndescription: {schema.description}\n"
    unique_list = "\n  - ".join(schema.unique_on)
    yml += f"unique_on: \n  - {unique_list}\n"
    yml += f"immutable: {schema.immutable}\nfields:\n"
    for f in schema.fields:
        y = field_to_yaml(f)
        yml += f"  {y}\n"
    return yml


#     # TODO:
#     # relations: List[Reference] = field(default_factory=list)
#     # implementations: List[SchemaName] = field(default_factory=list)


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
    from snapflow.core.module import DEFAULT_LOCAL_MODULE_NAME

    defaults: Dict[str, Any] = dict(
        name=name,
        module_name=DEFAULT_LOCAL_MODULE_NAME,
        version="1.0",
        description="...",
        unique_on=[],
        implementations=[],
    )
    defaults.update(kwargs)
    defaults["fields"] = [create_quick_field(f[0], f[1]) for f in fields]
    schema = Schema(**defaults)  # type: ignore
    return schema


AnySchema = Schema(
    name="Any",
    module_name="core",  # TODO: out of place?
    version="0",
    description="The Any root/super schema is compatible with all other Schemas",
    unique_on=[],
    fields=[],
)


class GenericSchemaException(Exception):
    pass
