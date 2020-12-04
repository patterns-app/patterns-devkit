from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from snapflow.core.data_formats.base import MemoryDataFormatBase

if TYPE_CHECKING:
    from snapflow.core.data_block import LocalMemoryDataRecords
    from snapflow.core.typing.schema import SchemaMapping, Schema


class DatabaseTableRef:
    def __init__(self, table_stmt_sql: str, storage_url: str):
        self.table_stmt_sql = table_stmt_sql
        self.storage_url = storage_url

    def __repr__(self):
        return f"{self.storage_url}/{self.table_stmt_sql}"

    def get_table_stmt(self) -> str:
        return self.table_stmt_sql


class DatabaseTableRefFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls):
        return DatabaseTableRef

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        # Not applicable to database table ref
        return obj

    @classmethod
    def apply_schema_mapping(
        cls, mapping: SchemaMapping, dtr: DatabaseTableRef
    ) -> DatabaseTableRef:
        """
        Apply mapping as a sub-select, aliasing column names
        """
        if not mapping.from_schema:
            raise NotImplementedError(
                f"Schema mapping must provide `from_schema` when mapping a db table {mapping}"
            )
        table_stmt = dtr.table_stmt_sql
        m = mapping.as_dict()
        col_stmts = []
        for f in mapping.from_schema.fields:
            col_stmts.append(f"{f.name} as {m.get(f.name, f.name)}")
        columns_stmt = ",".join(col_stmts)
        sql = f"""
        (
            select
                {columns_stmt}
            from {table_stmt}
        )
        """
        return DatabaseTableRef(table_stmt_sql=sql, storage_url=dtr.storage_url)
