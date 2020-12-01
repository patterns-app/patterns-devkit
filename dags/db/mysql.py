from typing import List, Optional

from sqlalchemy.engine import Engine

from dags.db.api import DatabaseAPI, conform_columns_for_insert
from loguru import logger

MYSQL_SUPPORTED = False
try:
    import mysqlclient

    MYSQL_SUPPORTED = True
except ImportError:

    pass


class MysqlDatabaseAPI(DatabaseAPI):
    pass
