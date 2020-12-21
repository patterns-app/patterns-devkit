import os
import re
from typing import Dict, List, Sequence

import sqlalchemy as sa
from jinja2 import nodes
from jinja2.ext import Extension
from snapflow.schema import Field, Schema
from snapflow.storage.storage import StorageEngine
from snapflow.utils.common import rand_str
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.ddl import CreateTable

re_comment = re.compile(r"(--|#).*")
re_table_ref = re.compile(r"\b(from|join)\s+(\w+\.)?\w+")


class SchemaFieldMapper:
    def get_field_type_class(self, ft: str) -> str:
        return ft.split("(")[0]

    def to_sqlalchemy(self, field: Field) -> Column:  # Sequence[Column]:
        # TODO: this list needs to be complete (since we use it for auto-detected / reflected types
        local_vars = {
            "TEXT": sa.UnicodeText,
            "VARCHAR": sa.Unicode,
            "Unicode": sa.Unicode,
            "UnicodeText": sa.UnicodeText,
            "Integer": sa.Integer,
            "BigInteger": sa.BigInteger,
            "Numeric": sa.Numeric,
            "Float": sa.Float,
            "REAL": sa.Float,
            "Boolean": sa.Boolean,
            "Date": sa.Date,
            "DATE": sa.Date,
            "DateTime": sa.DateTime,
            "Json": sa.JSON,
            "JSON": sa.JSON,
        }
        # TODO: constraints, indexes
        # TODO: "loose" vs "strict" typed columns (pre and post validation, right?)
        field_type = field.field_type
        ft_class = self.get_field_type_class(field_type)
        if ft_class not in local_vars:
            raise NotImplementedError(field.field_type)
        sa_data_type = eval(field_type, {"__builtins__": None}, local_vars)
        return Column(field.name, sa_data_type)

    def from_sqlalchemy(self, sa_column: Column, **kwargs) -> Field:
        # TODO: clean up reflected types? Conform / cast to standard set?
        return Field(name=sa_column.name, field_type=repr(sa_column.type), **kwargs)


class SchemaMapper:
    def __init__(self, sqlalchemy_metadata: MetaData = None):
        self.sqlalchemy_metadata = sqlalchemy_metadata or MetaData()

    def to_sqlalchemy(
        self,
        schema: Schema,
        schema_field_mapper: SchemaFieldMapper = None,
    ) -> Sequence[Column]:
        columns: List[Column] = []
        if schema_field_mapper is None:
            schema_field_mapper = SchemaFieldMapper()
        fields = schema.fields
        for field in fields:
            c = schema_field_mapper.to_sqlalchemy(field)
            columns.append(c)
        # TODO: table level constraints
        return columns

    def create_table_statement(
        self,
        schema: Schema,
        dialect: Dialect,
        table_name: str = None,
    ):
        sa_columns = self.to_sqlalchemy(schema)
        if not table_name:
            table_name = f"_{schema.name}_{rand_str(6)}"
        sa_table = Table(table_name, self.sqlalchemy_metadata, *sa_columns)
        stmt = CreateTable(sa_table).compile(dialect=dialect)
        sql = str(stmt)
        return sql

    def from_sqlalchemy(self, sa_table: Table, **kwargs) -> Schema:
        fields = kwargs.get("fields", [])
        field_mapper = SchemaFieldMapper()
        for column in sa_table.columns:
            fields.append(field_mapper.from_sqlalchemy(column))
        kwargs["fields"] = fields
        return Schema(**kwargs)


def field_from_sqlalchemy_column(sa_column: Column, **kwargs) -> Field:
    # TODO: clean up reflected types? Conform / cast to standard set?
    return Field(name=sa_column.name, field_type=repr(sa_column.type), **kwargs)


def fields_from_sqlalchemy_table(sa_table: Table) -> List[Field]:
    fields = []
    for column in sa_table.columns:
        fields.append(field_from_sqlalchemy_column(column))
    return fields
