from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from snapflow.storage.data_formats.base import MemoryDataFormatBase

if TYPE_CHECKING:
    from snapflow.schema import SchemaTranslation, Schema


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
    def apply_schema_translation(
        cls, translation: SchemaTranslation, dtr: DatabaseTableRef
    ) -> DatabaseTableRef:
        from snapflow.storage.db.utils import column_map

        """
        Apply translation as a sub-select, aliasing column names
        """
        if not translation.from_schema:
            raise NotImplementedError(
                f"Schema translation must provide `from_schema` when translating a db table {translation}"
            )
        sql = column_map(
            dtr.table_stmt_sql,
            translation.from_schema.field_names(),
            translation.as_dict(),
        )
        table_stmt = f"""
        (
            {sql}
        ) as __translated
        """
        return DatabaseTableRef(table_stmt_sql=table_stmt, storage_url=dtr.storage_url)
