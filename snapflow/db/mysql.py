from snapflow import RecordsList
from snapflow.db.api import DatabaseAPI, conform_columns_for_insert
from snapflow.utils.data import conform_records_for_insert

MYSQL_SUPPORTED = False
try:
    import mysqlclient

    MYSQL_SUPPORTED = True
except ImportError:
    pass


class MysqlDatabaseAPI(DatabaseAPI):
    def _bulk_insert(self, table_name: str, records: RecordsList):
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
