from typing import Dict
from datacopy.data_format.formats.memory.dataframe import pandas_series_to_field_type
from datacopy.utils.common import title_to_snake_case
from sqlalchemy.sql.schema import Column
from snapflow.core.metadata.orm import BaseModel
from openmodel.base import Schema, SchemaLike, create_quick_schema
from sqlalchemy.sql.sqltypes import JSON, String
import pandas as pd


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


class GenericSchemaException(Exception):
    pass


# TODO: move to openmodel?
def dict_to_rough_schema(name: str, d: Dict, convert_to_snake_case=True, **kwargs):
    fields = []
    for k, v in d.items():
        if convert_to_snake_case:
            k = title_to_snake_case(k)
        fields.append((k, pandas_series_to_field_type(pd.Series([v]))))
    fields = sorted(fields)
    return create_quick_schema(name, fields, **kwargs)
