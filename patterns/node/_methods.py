from __future__ import annotations

from typing import Iterator, Any, List, TYPE_CHECKING


if TYPE_CHECKING:
    try:
        from commonmodel import Schema
    except ImportError:
        Schema = None
    try:
        from pandas import DataFrame
    except ImportError:
        DataFrame = None


class TableVersion:
    @property
    def name(self) -> str:
        ...

    @property
    def storage(self):
        ...

    @property
    def schema(self) -> Schema | None:
        ...

    @property
    def record_count(self) -> int | None:
        ...

    @property
    def exists(self) -> bool:
        ...


class InputTableMethods:
    @classmethod
    def read(
        cls,
        as_format: str = "records",
        chunksize: int | None = None,
    ) -> List[dict] | DataFrame | Iterator[List[dict]] | Iterator[DataFrame]:
        """Reads records from this table.

        Args:
            as_format: Format to return records in. Defaults to list of dicts ('records').
                Set to 'dataframe' to get pandas dataframe (equivalent to ``read_dataframe``)
            chunksize: If specified, returns an iterator of the requested format in chunks of given size
        """
        ...

    @classmethod
    def read_dataframe(
        cls,
        chunksize: int | None = None,
    ) -> DataFrame | Iterator[DataFrame]:
        """Returns records as a pandas dataframe. Equivalent to `.read(as_format='dataframe')`

        Args:
            chunksize: If specified, returns an iterator of the dataframes of given size
        """
        ...

    @classmethod
    def read_sql(
        cls,
        sql: str,
        as_format: str = "records",
        chunksize: int | None = None,
    ) -> List[dict] | DataFrame | Iterator[List[dict]] | Iterator[DataFrame]:
        """Reads records resulting from the given sql expression, in same manner as ``read``.
        To reference tables in the sql, you can get their current (fully qualified and quoted)
        sql name by referencing `.sql_name` or, equivalently, taking their str() representation::

            my_table = Table("my_table")
            my_table.read_sql(f'select * from {my_table} limit 10')

        Args:
            sql: The sql select statement to execute
            as_format: Format to return records in. Defaults to list of dicts ('records').
                Set to 'dataframe' to get pandas dataframe.
            chunksize: If specified, returns an iterator of the requested format in chunks of given size
        """
        ...

    @classmethod
    def reset(cls):
        """Resets the table. No data is deleted on disk, but the active version of the
        table is reset to None.
        """
        ...

    @classmethod
    def get_active_version(cls) -> TableVersion | None:
        ...

    @classmethod
    def has_active_version(cls) -> bool:
        ...

    @property
    def is_connected(cls) -> bool:
        """Returns true if this table port is connected to a store in the graph. Operations
        on unconnected tables are no-ops and return dummy objects.
        """
        ...

    @property
    def sql_name(cls) -> str | None:
        """The fully qualified and quoted sql name of the active table version. The table may
        or may not exist on disk yet.
        """
        ...

    @property
    def schema(cls) -> Schema | None:
        """The Schema of the active table version. May be None"""
        ...

    @property
    def record_count(cls) -> int | None:
        """The record count of the active table version. May be None"""
        ...

    @property
    def exists(self) -> bool:
        """Returns True if the table has been created on disk."""
        ...


class OutputTableMethods:
    @classmethod
    def init(
        cls,
        schema: Schema | str | dict | None = None,
        schema_hints: dict[str, str] | None = None,
        unique_on: str | list[str] | None = None,
        # add_created: str | None = None,
        # add_updated: str | None = None,
        # add_monotonic_id: str | None = None,
        # indexes: list[str] | None = None,
        auto_indexes: bool = True,
    ):
        """Provides properties for this table that are used when a table version is first created on disk.

        Args:
            schema: A CommonModel Schema object or str name, or a dictionary of field names to field types
            schema_hints: A dictionary of field names to CommonModel field types that are used to override any inferred types. e.g. {"field1": "Text", "field2": "Integer"}
            unique_on: A field name or list of field names to that records should be unique on. Used by components
                to operate efficiently and correctly on the table.
            auto_indexes: If true (the default), an index is automatically created on new table
                versions for the `unique_on` property
        """

    @classmethod
    def append(cls, records: DataFrame | List[dict] | dict):
        """Appends the records to the end of this table. If this is the first
        write to this table then any schema provided is used to create the table,
        otherwise the schema is inferred from the passed in records.

        Args:
            records: May be list of dicts (with str keys) or a pandas dataframe.
        """
        ...

    @classmethod
    def upsert(cls, records: DataFrame | List[dict] | dict):
        """Upserts the records into this table, inserting new rows or
        updating if unique key conflicts. Unique fields must be provided by the Schema
        or passed to ``init``. If this is the first write to this table then any schema
        provided is used to create the table, otherwise the schema is inferred from the
        passed in records.

        Args:
            records: May be list of dicts (with str keys) or a pandas dataframe.
        """
        ...

    @classmethod
    def truncate(cls):
        """Truncates this table, preserving the table and schema on disk, but deleting all rows.
        Unlike ``reset`, which sets the active TableVersion to a new version, this action is
        destructive and cannot be undone.
        """
        ...

    @classmethod
    def execute_sql(cls, sql: str):
        """Executes the given sql against the database this table is stored on. The sql is inspected
        to determine if it creates new tables or only modifies them, and appropriate events are recorded.
        The sql should ONLY create or update THIS table. Creating or updating other tables will result in
        incorrect event propagation.

        To reference tables in the sql, you can get their current (fully qualified and quoted)
        sql name by referencing `.sql_name` or, equivalently, taking their str() representation::

            my_table = Table("my_table", "w")
            my_table.execute_sql(f'create table {my_table} as select 1 as a, 2 as b')

        Args:
            sql: Any valid sql statement that creates, inserts, updates, or otherwise alters this table.
        """
        ...

    @classmethod
    def create_new_version(cls) -> TableVersion:
        ...

    @classmethod
    def get_active_version(cls) -> TableVersion | None:
        ...

    @classmethod
    def set_active_version(cls, table: TableVersion):
        ...

    @classmethod
    def signal_create(cls):
        ...

    @classmethod
    def signal_update(cls):
        ...

    @classmethod
    def signal_reset(cls):
        ...

    @classmethod
    def reset(cls):
        """Resets this table to point to a new (null) TableVersion with no Schema or data. Schema
        and data of previous version still exist on disk until garbage collected according to the
        table's retention policy."""
        ...


class InputStreamMethods:
    @classmethod
    def consume_records(cls, with_metadata: bool = False) -> Iterator[dict]:
        """Iterates over records in this stream one at a time. When a record
        is yielded it is marked as consumed, regardless of what happens after.
        If you want to recover from errors and have the option to re-process records,
        you can use ``rollback`` and ``checkpoint`` explicitly in a try / except block.
        """
        ...

    def __iter__(cls) -> Iterator[dict]:
        ...

    @classmethod
    def checkpoint(cls):
        ...

    @classmethod
    def rollback(cls, records_to_rollback: int = 1):
        ...

    @classmethod
    def reset(cls):
        """Resets (empties) this stream."""
        ...


class OutputStreamMethods:
    @classmethod
    def append(cls, record: DataFrame | List[dict] | dict):
        ...


class StateMethods:
    @classmethod
    def set(cls, state: dict):
        ...

    @classmethod
    def set_value(cls, key: str, value: Any):
        ...

    @classmethod
    def get(cls) -> dict:
        ...

    @classmethod
    def get_value(cls, key: str, default: Any = None) -> Any:
        ...

    @classmethod
    def get_datetime(cls, key: str, default: Any = None) -> Any:
        ...

    @classmethod
    def should_continue(
        cls, pct_of_limit: float = None, seconds_till_limit: int = None
    ) -> bool:
        ...

    @classmethod
    def request_new_run(
        cls, trigger_downstream: bool = True, wait_atleast_seconds: int = None
    ):
        ...

    @classmethod
    def reset(cls):
        ...


class ParameterMethods:
    pass
