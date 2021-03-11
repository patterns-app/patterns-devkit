import os
import re
from typing import Dict, List, Sequence

import sqlalchemy as sa
from jinja2 import nodes
from jinja2.ext import Extension
from snapflow.schema import Field, Schema
from snapflow.schema.field_types import ensure_field_type
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
        return Column(field.name, field.field_type.as_sqlalchemy_type())

    def from_sqlalchemy(self, sa_column: Column, **kwargs) -> Field:
        # TODO: clean up reflected types? Conform / cast to standard set?
        return Field(
            name=sa_column.name, field_type=ensure_field_type(sa_column.type), **kwargs
        )


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
