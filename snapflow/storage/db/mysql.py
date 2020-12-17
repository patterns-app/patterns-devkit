from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List

from snapflow.storage.db.api import (
    DatabaseApi,
    DatabaseStorageApi,
    create_db,
    dispose_all,
    drop_db,
)
from snapflow.storage.db.utils import conform_columns_for_insert
from snapflow.utils.common import rand_str
from snapflow.utils.data import conform_records_for_insert

MYSQL_SUPPORTED = False
try:
    import MySQLdb

    MYSQL_SUPPORTED = True
except ImportError:
    pass


class MysqlDatabaseApi(DatabaseApi):
    def dialect_is_supported(self) -> bool:
        return MYSQL_SUPPORTED

    def _bulk_insert(self, table_name: str, records: List[Dict]):
        columns = conform_columns_for_insert(records)
        records = conform_records_for_insert(records, columns)
        sql = f"""
        INSERT INTO `{ table_name }` (
            `{ '`,`'.join(columns)}`
        ) VALUES ({','.join(['%s'] * len(columns))})
        """
        conn = self.get_engine().raw_connection()
        curs = conn.cursor()
        try:
            curs.executemany(sql, records)
            conn.commit()
        finally:
            conn.close()

    @classmethod
    @contextmanager
    def temp_local_database(cls) -> Iterator[str]:
        test_db = f"__tmp_snapflow_{rand_str(8).lower()}"
        url = "mysql://root@localhost"
        create_db(url, test_db)
        test_url = f"{url}/{test_db}"
        try:
            yield test_url
        finally:
            dispose_all(test_db)
            drop_db(url, test_db)


class MysqlDatabaseStorageApi(DatabaseStorageApi, MysqlDatabaseApi):
    pass
