from snapflow.schema.base import Schema
from snapflow.storage.data_copy.base import (
    Conversion,
    NetworkToBufferCost,
    NetworkToMemoryCost,
    NoOpCost,
    datacopy,
)
from snapflow.storage.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRef,
    DatabaseTableRefFormat,
    RecordsFormat,
)
from snapflow.storage.data_formats.records import RecordsIteratorFormat
from snapflow.storage.data_records import as_records
from snapflow.storage.db.api import DatabaseStorageApi
from snapflow.storage.db.utils import result_proxy_to_records
from snapflow.storage.storage import (
    DatabaseStorageClass,
    PythonStorageApi,
    PythonStorageClass,
    StorageApi,
)


@datacopy(
    from_storage_classes=[DatabaseStorageClass],
    from_data_formats=[DatabaseTableFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsFormat],
    cost=NetworkToMemoryCost,
)
def copy_db_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, DatabaseStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    select_sql = f"select * from {from_name}"
    with from_storage_api.execute_sql_result(select_sql) as r:
        records = result_proxy_to_records(r)
        mdr = as_records(records, data_format=RecordsFormat, schema=schema)
        mdr = mdr.conform_to_schema()
        to_storage_api.put(to_name, mdr)


@datacopy(
    from_storage_classes=[DatabaseStorageClass],
    from_data_formats=[DatabaseTableFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[RecordsIteratorFormat],
    cost=NetworkToBufferCost,
)
def copy_db_to_records_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, DatabaseStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    select_sql = f"select * from {from_name}"
    conn = (
        from_storage_api.get_engine().connect()
    )  # Gonna leave this connection hanging... # TODO: add "closeable" to the MDR and handle?
    r = conn.execute(select_sql)

    def f():
        while True:
            # TODO: how to parameterize this chunk size? (it's approximate anyways for some dbs?)
            rows = r.fetchmany(1000)
            if not rows:
                return
            records = result_proxy_to_records(r, rows=rows)
            yield records

    mdr = as_records(f(), data_format=RecordsIteratorFormat, schema=schema)
    mdr = mdr.conform_to_schema()
    mdr.closeable = conn.close
    to_storage_api.put(to_name, mdr)


@datacopy(
    from_storage_classes=[DatabaseStorageClass],
    from_data_formats=[DatabaseTableFormat],
    to_storage_classes=[PythonStorageClass],
    to_data_formats=[DatabaseCursorFormat],
    cost=NetworkToBufferCost,
)
def copy_db_to_cursor(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, DatabaseStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    select_sql = f"select * from {from_name}"
    conn = (
        from_storage_api.get_engine().connect()
    )  # Gonna leave this connection hanging... # TODO: add "closeable" to the MDR and handle?
    r = conn.execute(select_sql)
    mdr = as_records(r, data_format=DatabaseCursorFormat, schema=schema)
    mdr = mdr.conform_to_schema()
    mdr.closeable = conn.close
    to_storage_api.put(to_name, mdr)


# @datacopy(
#     from_storage_classes=[DatabaseStorageClass],
#     from_data_formats=[DatabaseTableFormat],
#     to_storage_classes=[PythonStorageClass],
#     to_data_formats=[DatabaseTableFormat],
#     cost=NoOpCost,
# )
# def copy_db_to_ref(
#     from_name: str,
#     to_name: str,
#     conversion: Conversion,
#     from_storage_api: StorageApi,
#     to_storage_api: StorageApi,
#     schema: Schema,
# ):
#     assert isinstance(from_storage_api, DatabaseStorageApi)
#     assert isinstance(to_storage_api, PythonStorageApi)
#     r = DatabaseTableRef(to_name, storage_url=from_storage_api.storage.url)
#     mdr = as_records(r, data_format=DatabaseTableFormat, schema=schema)
#     to_storage_api.put(to_name, mdr)
