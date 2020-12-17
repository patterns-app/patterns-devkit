from contextlib import contextmanager
from typing import Dict, Iterator, List

from loguru import logger
from snapflow.storage.db.api import (
    DatabaseApi,
    DatabaseStorageApi,
    create_db,
    dispose_all,
    drop_db,
)
from snapflow.storage.db.utils import (
    compile_jinja_sql_template,
    conform_columns_for_insert,
)
from snapflow.utils.common import rand_str
from snapflow.utils.data import conform_records_for_insert
from sqlalchemy.engine import Engine

POSTGRES_SUPPORTED = False
try:
    from psycopg2.extras import execute_values

    POSTGRES_SUPPORTED = True
except ImportError:

    def execute_values(*args):
        raise ImportError("Psycopg2 not installed")


def bulk_insert(*args, **kwargs):
    kwargs["update"] = False
    return bulk_upsert(*args, **kwargs)


def bulk_upsert(
    eng: Engine,
    table_name: str,
    records: List[Dict],
    unique_on_column: str = None,
    ignore_duplicates: bool = False,
    update: bool = True,
    columns: List[str] = None,
    adapt_objects_to_json: bool = True,
    page_size: int = 5000,
):
    if not records:
        return
    if update and not unique_on_column:
        raise Exception("Must specify unique_on_column when updating")
    columns = conform_columns_for_insert(records, columns)
    records = conform_records_for_insert(records, columns, adapt_objects_to_json)
    if update:
        tmpl = "bulk_upsert.sql"
    else:
        tmpl = "bulk_insert.sql"
    jinja_ctx = {
        "table_name": table_name,
        "columns": columns,
        "records": records,
        "unique_on_column": unique_on_column,
        "ignore_duplicates": ignore_duplicates,
    }
    sql = compile_jinja_sql_template(tmpl, jinja_ctx)
    logger.debug("SQL", sql)
    pg_execute_values(eng, sql, records, page_size=page_size)


def pg_execute_values(
    eng: Engine, sql: str, records: List[Dict], page_size: int = 5000
):
    conn = eng.raw_connection()
    try:
        with conn.cursor() as curs:
            execute_values(
                curs,
                sql,
                records,
                template=None,
                page_size=page_size,
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


class PostgresDatabaseApi(DatabaseApi):
    def dialect_is_supported(self) -> bool:
        return POSTGRES_SUPPORTED

    def _bulk_insert(self, table_name: str, records: List[Dict], **kwargs):
        bulk_insert(
            eng=self.get_engine(), table_name=table_name, records=records, **kwargs
        )

    @classmethod
    @contextmanager
    def temp_local_database(cls) -> Iterator[str]:
        test_db = f"__tmp_snapflow_{rand_str(8).lower()}"
        url = "postgresql://localhost"
        pg_url = f"{url}/postgres"
        create_db(pg_url, test_db)
        test_url = f"{url}/{test_db}"
        try:
            yield test_url
        finally:
            dispose_all(test_db)
            drop_db(pg_url, test_db)


class PostgresDatabaseStorageApi(DatabaseStorageApi, PostgresDatabaseApi):
    pass
