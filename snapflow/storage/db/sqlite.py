from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List

from snapflow.storage.db.api import DatabaseApi, DatabaseStorageApi
from snapflow.storage.db.utils import conform_columns_for_insert, get_tmp_sqlite_db_url
from snapflow.utils.data import conform_records_for_insert


class SqliteDatabaseApi(DatabaseApi):
    @classmethod
    @contextmanager
    def temp_local_database(cls) -> Iterator[str]:
        db_url = get_tmp_sqlite_db_url("__test_snapflow_sqlite")
        yield db_url


class SqliteDatabaseStorageApi(DatabaseStorageApi, SqliteDatabaseApi):
    pass
