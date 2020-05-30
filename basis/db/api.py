import json
import logging
from typing import List, Type, Union

import sqlalchemy
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import OperationalError, ProgrammingError

from basis.core.data_block import StoredDataBlockMetadata
from basis.core.data_format import DictList
from basis.core.environment import Environment
from basis.core.runtime import Runtime
from basis.core.sql.utils import ObjectTypeMapper
from basis.core.storage import Storage, StorageEngine
from basis.utils.common import JSONEncoder, printd, title_to_snake_case

logger = logging.getLogger(__name__)


def conform_records_for_insert(
    records: DictList, columns: List[str], adapt_objects_to_json: bool = True,
):
    rows = []
    for r in records:
        row = []
        for c in columns:
            o = r.get(c)
            # TODO: this is some magic buried down here. no bueno
            if adapt_objects_to_json and (isinstance(o, list) or isinstance(o, dict)):
                o = json.dumps(o, cls=JSONEncoder)
            row.append(o)
        rows.append(row)
    return rows


def conform_columns_for_insert(
    records: DictList,
    columns: List[str] = None,
    convert_columns_to_snake_case: bool = False,
) -> List[str]:
    if columns is None:
        # Use first object's keys as columns. Assumes uniform dicts
        columns = list(records[0].keys())
    if convert_columns_to_snake_case:
        columns = [
            title_to_snake_case(c) for c in columns
        ]  # TODO: DO NOT DO THIS HERE! Just quote things properly in the SQL. smh...
    return columns


class DatabaseAPI:
    def __init__(self, env: Environment, resource: Union[Runtime, Storage]):
        self.env = env
        self.resource = resource

    def get_connection(self) -> sqlalchemy.engine.Engine:
        return sqlalchemy.create_engine(self.resource.url)

    def execute_sql(self, sql: str) -> ResultProxy:
        printd("Executing SQL:")
        printd(sql)
        return self.get_connection().execute(sql)

    def ensure_table(self, sdb: StoredDataBlockMetadata) -> str:
        name = sdb.get_name(self.env)
        if self.exists(name):
            return name
        otype = sdb.get_otype(self.env)
        ddl = ObjectTypeMapper(self.env).create_table_statement(
            otype=otype, storage_engine=sdb.storage.storage_engine, table_name=name,
        )
        self.execute_sql(ddl)
        return name

    def insert_sql(self, destination_sdb: StoredDataBlockMetadata, sql: str):
        name = self.ensure_table(destination_sdb)
        otype = destination_sdb.get_otype(self.env)
        columns = "\n,".join(f.name for f in otype.fields)
        insert_sql = f"""
        insert into {name} (
            {columns}
        )
        select
        {columns}
        from (
        {sql}
        ) as __sub
        """
        self.execute_sql(insert_sql)

    def bulk_insert_dict_list(
        self, destination_sdb: StoredDataBlockMetadata, records: DictList
    ):
        # Create table whether or not there is anything to insert (side-effect consistency)
        name = self.ensure_table(destination_sdb)
        if not records:
            return
        self._bulk_insert(name, records)

    def _bulk_insert(self, table_name: str, records: DictList):
        raise NotImplementedError(
            f"Bulk insert is not implemented for {self.resource.url}"
        )

    def exists(self, table_name: str) -> bool:
        try:
            self.execute_sql(f"select * from {table_name} limit 0")
            return True
        except (OperationalError, ProgrammingError) as x:
            if "does not exist" in str(x).lower():
                return False
            raise x

    def count(self, table_name: str) -> int:
        res = self.execute_sql(f"select count(*) from {table_name}")
        return res.fetchone()[0]


def get_database_api_class(engine: StorageEngine) -> Type[DatabaseAPI]:
    from basis.db.postgres import PostgresDatabaseAPI

    return {
        StorageEngine.POSTGRES: PostgresDatabaseAPI,
        # StorageEngine.MYSQL: MysqlDatabaseAPI, # TODO
    }.get(engine, DatabaseAPI)
