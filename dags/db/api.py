from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Callable,
    ContextManager,
    Generator,
    List,
    Tuple,
    Type,
    Union,
)

import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.engine import Connection, Engine, ResultProxy
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session

from dags.core.data_block import DataBlockMetadata, StoredDataBlockMetadata
from dags.core.data_formats import DatabaseTableFormat, DataFormat, RecordsList
from dags.core.environment import Environment
from dags.core.runtime import Runtime
from dags.core.sql.utils import ObjectTypeMapper
from dags.core.storage.storage import Storage, StorageEngine
from dags.core.typing.inference import infer_otype_from_db_table
from dags.core.typing.object_type import ObjectType, is_any
from dags.utils.common import DagsJSONEncoder, printd, rand_str, title_to_snake_case
from loguru import logger

if TYPE_CHECKING:
    pass


# _sa_engines: Dict[str, Engine] = {}
_sa_engines: List[Engine] = []


def dispose_all():
    for e in _sa_engines:
        e.dispose()


def conform_columns_for_insert(
    records: RecordsList,
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
    def __init__(
        self,
        env: Environment,
        resource: Union[Runtime, Storage],
        json_serializer: Callable = None,
    ):
        self.env = env
        self.resource = resource
        self.json_serializer = (
            json_serializer
            if json_serializer is not None
            else lambda o: json.dumps(o, cls=DagsJSONEncoder)
        )

    def get_engine(self) -> sqlalchemy.engine.Engine:
        # return sqlalchemy.create_engine(
        #     self.resource.url, json_serializer=self.json_serializer
        # )
        url = self.resource.url
        # try:
        #     return _sa_engines[url]
        # except KeyError:
        eng = sqlalchemy.create_engine(
            url, json_serializer=self.json_serializer, echo=False
        )
        _sa_engines.append(eng)
        return eng

    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        with self.get_engine().connect() as conn:
            yield conn

    def execute_sql(self, sql: str) -> ResultProxy:
        logger.debug("Executing SQL:")
        logger.debug(sql)
        with self.connection() as conn:
            return conn.execute(sql)

    def ensure_table(self, sdb: StoredDataBlockMetadata) -> str:
        name = sdb.get_name(self.env)
        if self.exists(name):
            return name
        otype = sdb.get_realized_otype(self.env)
        ddl = ObjectTypeMapper(self.env).create_table_statement(
            otype=otype, storage_engine=sdb.storage.storage_engine, table_name=name,
        )
        self.execute_sql(ddl)
        return name

    def rename_table(self, table_name: str, new_name: str):
        self.execute_sql(f"alter table {table_name} rename to {new_name}")

    def insert_sql(self, destination_sdb: StoredDataBlockMetadata, sql: str):
        name = self.ensure_table(destination_sdb)
        otype = destination_sdb.get_realized_otype(self.env)
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

    def create_data_block_from_sql(
        self, sess: Session, sql: str, expected_otype: ObjectType = None
    ) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
        tmp_name = f"_tmp_{rand_str(10)}".lower()
        create_sql = f"""
        create table {tmp_name} as
        select
        *
        from (
        {sql}
        ) as __sub
        """
        self.execute_sql(create_sql)
        cnt = self.count(tmp_name)
        # TODO: DRY this with other "create_data_block"
        if not expected_otype:
            expected_otype = self.env.get_otype("Any")
        expected_otype_key = expected_otype.key
        if is_any(expected_otype):
            realized_otype = infer_otype_from_db_table(self, tmp_name)
            self.env.add_new_otype(realized_otype)
        else:
            realized_otype = expected_otype
        realized_otype_key = realized_otype.key
        block = DataBlockMetadata(
            expected_otype_key=expected_otype_key,
            realized_otype_key=realized_otype_key,
            record_count=cnt,
        )
        storage_url = self.resource.url
        sdb = StoredDataBlockMetadata(
            data_block=block, storage_url=storage_url, data_format=DatabaseTableFormat,
        )
        sess.add(block)
        sess.add(sdb)
        # TODO: Don't understand merge still
        block = sess.merge(block)
        sdb = sess.merge(sdb)
        # TODO: would be great to validate that a DB/SDB resource is named right, or even to record the name
        #   eg what if we change the naming logic at some point...?  attr on SDB: storage_name or something
        self.rename_table(tmp_name, sdb.get_name(self.env))
        return block, sdb

    def bulk_insert_records_list(
        self, destination_sdb: StoredDataBlockMetadata, records: RecordsList
    ):
        # Create table whether or not there is anything to insert (side-effect consistency)
        name = self.ensure_table(destination_sdb)
        if not records:
            return
        self._bulk_insert(name, records)

    def _bulk_insert(self, table_name: str, records: RecordsList):
        raise NotImplementedError(
            f"Bulk insert is not implemented for {self.resource.url}"
        )

    def exists(self, table_name: str) -> bool:
        try:
            self.execute_sql(f"select * from {table_name} limit 0")
            return True
        except (OperationalError, ProgrammingError) as x:
            s = str(x).lower()
            if "does not exist" in s or "no such" in s:
                return False
            raise x

    def count(self, table_name: str) -> int:
        res = self.execute_sql(f"select count(*) from {table_name}")
        row = res.fetchone()
        if not row:
            raise
        return row[0]

    def get_sqlalchemy_metadata(self):
        sa_engine = self.get_engine()
        meta = MetaData()
        meta.reflect(bind=sa_engine)
        return meta


# TODO: better way to register these types of managers / apis (so someone can extend without editing
def get_database_api_class(engine: StorageEngine) -> Type[DatabaseAPI]:
    from dags.db.postgres import PostgresDatabaseAPI

    return {
        StorageEngine.POSTGRES: PostgresDatabaseAPI,
        # StorageEngine.MYSQL: MysqlDatabaseAPI, # TODO
    }.get(engine, DatabaseAPI)


def create_db(url: str, database_name: str):
    if url.startswith("sqlite"):
        logger.info("create_db is no-op for sqlite")
        return
    sa = sqlalchemy.create_engine(url)
    conn = sa.connect()
    try:
        conn.execute(
            "commit"
        )  # Close default open transaction (can't create db inside tx)
        conn.execute(f"create database {database_name}")
    finally:
        conn.close()


def drop_db(url: str, database_name: str):
    if url.startswith("sqlite"):
        return drop_sqlite_db(url, database_name)
    if "test" not in database_name:
        i = input("Dropping db {database_name}, are you sure? (y/N)")
        if not i.lower().startswith("y"):
            return
    sa = sqlalchemy.create_engine(url)
    conn = sa.connect()
    try:
        conn.execute(
            "commit"
        )  # Close default open transaction (can't drop db inside tx)
        conn.execute(f"drop database {database_name}")
    finally:
        conn.close()


def drop_sqlite_db(url: str, database_name: str):
    if database_name == ":memory:":
        return
    os.remove(database_name)
