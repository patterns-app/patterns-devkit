from __future__ import annotations

from datetime import datetime, date
from typing import Iterator, Any, List, TYPE_CHECKING
from typing import TypeVar, Type

if TYPE_CHECKING:
    try:
        from commonmodel import Schema
    except ImportError:
        Schema = None
    try:
        from pandas import DataFrame
    except ImportError:
        DataFrame = None


class Stream:
    """A stateful view of a Table that supports consuming the table in a
    one-record-at-a-time manner in a given ordering, preserving progress
    across executions for the given node.

    Example::

        table = Table("my_table")
        stream = table.as_stream(order_by="id")
        for record in stream.consume_records():
            print(record)

        # Rewind and the stream will consume from the beginning again
        stream.rewind()
        for record in stream.consume_records():
            print(record)

        stream.seek(42)
        for record in stream.consume_records():
            print(record) # only values *greater* than 42
    """

    @classmethod
    def consume_records(cls, with_metadata: bool = False) -> Iterator[dict]:
        """Iterate over records in this stream one at a time.

        When a record is yielded it is marked as consumed, regardless of what happens after.
        If you want to recover from errors and have the option to re-process records,
        you can use ``rollback`` and ``checkpoint`` explicitly in a try / except block.
        """
        ...

    def __iter__(cls) -> Iterator[dict]:
        """Equivalent to ``consume_records``"""
        ...

    @classmethod
    def checkpoint(cls):
        """Save the stream state (which records have been consumed from the iterator)
        to disk."""
        ...

    @classmethod
    def rollback(cls):
        """Roll back stream to beginning of execution or last ``checkpoint``."""
        ...

    @classmethod
    def rewind(self):
        """Reset the stream to consume from the beginning again"""
        ...

    @classmethod
    def seek(self, value: Any):
        """Seek to the given value (of the order_by field).

        Stream will consume values strictly *greater* than the given value, not including
        any record equal to the given value."""
        ...

    @property
    def order_by_field(self) -> str:
        """Return the ordering field for this stream"""
        ...


class TableVersion:
    """A specific version of a Table, representing an actual database table
    that may or may not be stored on disk yet.

    A Table may have many TableVersions, one or zero of which will be active
    at any given time."""

    @property
    def name(self) -> str:
        """The unqualified name of the table."""
        ...

    @property
    def storage(self):
        """The dcp Storage object this table is stored on."""
        ...

    @property
    def schema(self) -> Schema | None:
        """The realized schema of this TableVersion. None if does not exist on disk."""
        ...

    @property
    def record_count(self) -> int | None:
        """The realized schema of this TableVersion. None if does not exist on disk."""
        ...

    @property
    def exists(self) -> bool:
        """True if this version exists on disk."""
        ...


class Table:
    def __init__(
        self,
        name: str,
        mode: str = "r",
        description: str = None,
        schema: str = None,
        required: bool = True,
    ):
        """Table is a thin abstraction over a database table that
        provides a stable reference across versions of the table.

        Args:
            name: The Patterns name for the table. The actual database table
                on disk will include this name and a hash.
            mode: Whether to use the table in "read" mode ("r") or "write" mode ("w")
            description: An optional short description of this table
            schema: An optional explicit Schema for this table. If not provided the
                schema will be inferred, or can be set with the table's `init` method.
            required: Whether this table is a required table for the operation of the
                node, or is optional.
        """
        pass

    @classmethod
    def read(
        cls,
        as_format: str = "records",
        chunksize: int | None = None,
    ) -> List[dict] | DataFrame | Iterator[List[dict]] | Iterator[DataFrame]:
        """Read records from this table.

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
    ) -> Iterator[DataFrame] | DataFrame:
        """Return records as a pandas dataframe. Equivalent to `.read(as_format='dataframe')`

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
        """Read records resulting from the given sql expression, in same manner as ``read``.

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
    def as_stream(cls, order_by: str = None, starting_value: Any = None) -> Stream:
        """Return a Stream over the given table that will consume each record in the
        table exactly once, in order.

        Progress along the stream is stored in the node's state. A table may have
        multiple simultaneous streams with different orderings. The stream is ordered
        by the `order_by` parameter if provided otherwise defaults to the schema's
        `strictly_monotonic_ordering` if defined or its `created_ordering` if defined.
        If none of those orderings exist, an exception is thrown.

        To add a convenient ordering to records when writing (if you plan on streaming
        the table downstream), you can use `table.init(add_monotonic_id="id")` or
        `table.init(add_created="created_at")`.

        Args:
            order_by: Optional, the field to order the stream by. If not provided
                defaults to schema-defined orderings
            starting_value: Optional, value on the order by field at which to start the stream

        Returns:
            Stream object.
        """
        ...

    @classmethod
    def reset(cls):
        """Reset the table.

        No data is deleted on disk, but the active version of the table is reset to None.
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
        """Return True if this table port is connected to a store in the graph.

        Operations on unconnected tables are no-ops and return dummy objects.
        """
        ...

    @property
    def sql_name(cls) -> str | None:
        """The fully qualified and quoted sql name of the active table version.

        The table may or may not exist on disk yet.
        """
        ...

    @property
    def schema(cls) -> Schema | None:
        """The Schema of the active table version.

        May be None.
        """
        ...

    @property
    def record_count(cls) -> int | None:
        """The record count of the active table version.

        May be None.
        """
        ...

    @property
    def exists(self) -> bool:
        """True if the table has been created on disk."""
        ...

    @classmethod
    def init(
        cls,
        schema: Schema | str | dict | None = None,
        schema_hints: dict[str, str] | None = None,
        unique_on: str | list[str] | None = None,
        add_created: str | None = None,
        add_monotonic_id: str | None = None,
        auto_indexes: bool = True,
    ):
        """Provide properties for this table that are used when a table version is first created on disk.

        Args:
            schema: A CommonModel Schema object or str name, or a dictionary of field names to field types
            schema_hints: A dictionary of field names to CommonModel field types that are used to override any inferred types. e.g. {"field1": "Text", "field2": "Integer"}
            unique_on: A field name or list of field names to that records should be unique on. Used by components
                to operate efficiently and correctly on the table.
            add_created: If specified, is the field name that an "auto_now" timestamp will be added to each
                record when `append` or `upsert` is called. This field
                will be the default streaming order for the table (by automatically filling the
                `created_ordering` role on the associated Schema), but only if add_monotonic_id is NOT specified
                and the associated schema defines no monotonic ordering.
            add_monotonic_id: If specified, is the field name that a unique, strictly monotonically increasing
                base32 string will be added to each record when `append` or `upsert` is called. This field
                will be the default streaming order for the table (by automatically filling the
                `strictly_monotonic_ordering` role on the associated Schema).
            auto_indexes: If true (the default), an index is automatically created on new table
                versions for the `unique_on` property
        """

    @classmethod
    def append(cls, records: DataFrame | List[dict] | dict):
        """Append the records to the end of this table.

        If this is the first write to this table then any schema provided is used to
        create the table, otherwise the schema is inferred from the passed in records.

        Records are buffered and written to disk in batches. To force an immediate write,
        call `table.flush()`.

        To replace a table with a new (empty) version and append from there, call
        `table.reset()`.

        Args:
            records: May be a list of records (list of dicts with str keys),
                a single record (dict), or a pandas dataframe.
        """
        ...

    @classmethod
    def upsert(cls, records: DataFrame | List[dict] | dict):
        """Upsert the records into this table, inserting new rows or
        updating if unique key conflicts.

        Unique fields must be provided by the Schema or passed to ``init``. If this is
        the first write to this table then any schema provided is used to create the table,
        otherwise the schema is inferred from the passed in records.

        Records are buffered and written to disk in batches. To force an immediate write,
        call `table.flush()`.

        Args:
            records: May be a list of records (list of dicts with str keys),
                a single record (dict), or a pandas dataframe.
        """
        ...

    @classmethod
    def replace(cls, records: DataFrame | List[dict]):
        """Replace the current table version (if any) with a new one containing just `records`.

        Equivalent to `table.reset(); table.append(records)`.

        Args:
            records: May be a list of records (list of dicts with str keys)
                or a pandas dataframe.
        """

    @classmethod
    def truncate(cls):
        """Truncate this table, preserving the table and schema on disk, but deleting all rows.

        Unlike ``reset`, which sets the active TableVersion to a new version, this action is
        destructive and cannot be undone.
        """
        ...

    @classmethod
    def execute_sql(cls, sql: str):
        """Execute the given sql against the database this table is stored on.

        The sql is inspected to determine if it creates new tables or only modifies them,
        and appropriate events are recorded. The sql should ONLY create or update THIS table.
        Creating or updating other tables will result in incorrect event propagation.

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
        """Reset this table to point to a new (null) TableVersion with no Schema or data.

        Schema and data of previous version still exist on disk until garbage collected according to the
        table's retention policy."""
        ...

    @classmethod
    def flush(cls):
        """Flush any buffered records to disk.

        Calls to table.append and table.upsert are buffered and flushed periodically
        and at the end of an execution. Use this method to force an immediate write.
        """
        ...


class State:
    """
    State is a wrapper around a Table that supports quickly storing
    and retrieving single values from the database.
    """

    @classmethod
    def set(cls, state: dict):
        """Replace the whole state dict with the provided one"""
        ...

    @classmethod
    def set_value(cls, key: str, value: Any):
        """Set the given value for the given key on this node's state."""
        ...

    @classmethod
    def get(cls) -> dict:
        """Get the current state dict"""
        ...

    @classmethod
    def get_value(cls, key: str, default: Any = None) -> Any:
        """Get the latest value from state for this node for the given key.

        Args:
            key: key for state value
            default: default value if key is not present in state

        Returns:
            value from state
        """
        ...

    @classmethod
    def get_datetime(cls, key: str, default: datetime = None) -> datetime | None:
        """Get the latest value from state for given key and tries
        to cast to a python datetime.

        Args:
            key: key for state
            default: default datetime if key is not present in state

        Returns:
            datetime from state or None
        """
        ...

    @classmethod
    def should_continue(
        cls, pct_of_limit: float = None, seconds_till_limit: int = None
    ) -> bool:
        """Return False if execution is near its hard time limit (10 minutes typically),
        otherwise returns True.

        Used to exit gracefully from long-running jobs, typically in conjunction with
        ``request_new_run``. Defaults to 80% of limit or 120 seconds before the
        hard limit, which ever is greater.

        Args:
            pct_of_limit: percent of time limit to trigger at
            seconds_till_limit: seconds before time limit to trigger at
        """
        ...

    @classmethod
    def request_new_run(
        cls, trigger_downstream: bool = True, wait_atleast_seconds: int = None
    ):
        """Request a new run from the server for this node, to be started
        once the current execution finishes.

        Often used in conjunction with ``should_continue`` to run long jobs
        over multiple executions safely.

        The requested run be delayed with `wait_atleast_seconds` to space out
        the executions.

        Args:
            trigger_downstream: Whether new run should trigger downstream nodes too
            wait_atleast_seconds: Time to wait until starting the new run

        """
        ...

    @classmethod
    def reset(cls):
        """Reset (clear) the state for this node."""
        ...


class Connection(dict):
    def __init__(self, connection_type: str):
        super().__init__()


T = TypeVar("T", str, int, float, bool, datetime, date, list, Connection)


def Parameter(
    description: str = None,
    type: Type[T] = str,
    default: T = "MISSING",
) -> T:
    """Parameters let a python script take values from the end user / UI.

    Allowed parameter types:

    * str
    * int
    * float
    * bool
    * datetime
    * date
    * list
    * Connection

    Args:
        description: Description / help text
        type: should be the actual python type, e.g. `type=str` or `type=datetime`
        default: default value. If not set explicitly, the parameter is assumed to be required.
            May be set to None
    """
    pass
