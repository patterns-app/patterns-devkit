from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable, Dict, Iterator, List, Optional, Tuple, Type

import sqlalchemy
from loguru import logger
from snapflow.core.typing.inference import infer_schema_from_db_table
from snapflow.schema.base import Schema
from snapflow.storage.data_formats.records import Records
from snapflow.storage.db.schema import SchemaMapper
from snapflow.storage.db.utils import conform_columns_for_insert
from snapflow.storage.storage import Storage, StorageApi
from snapflow.utils.common import SnapflowJSONEncoder, rand_str
from snapflow.utils.data import conform_records_for_insert
from sqlalchemy import MetaData
from sqlalchemy.engine import Connection, Engine, ResultProxy
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm.session import Session

if TYPE_CHECKING:
    pass


_sa_engines: Dict[str, Engine] = {}


def dispose_all(keyword: Optional[str] = None):
    for k, e in _sa_engines.items():
        if keyword:
            if keyword not in str(e.url):
                continue
        e.dispose()


class DatabaseApi:
    def __init__(
        self,
        url: str,
        json_serializer: Callable = None,
    ):
        self.url = url
        self.json_serializer = (
            json_serializer
            if json_serializer is not None
            else lambda o: json.dumps(o, cls=SnapflowJSONEncoder)
        )
        self.eng: Optional[sqlalchemy.engine.Engine] = None

    def _get_engine_key(self) -> str:
        return f"{self.url}_{self.json_serializer.__class__.__name__}"

    def get_engine(self) -> sqlalchemy.engine.Engine:
        if self.eng is not None:
            return self.eng
        key = self._get_engine_key()
        if key in _sa_engines:
            return _sa_engines[key]
        self.eng = sqlalchemy.create_engine(
            self.url,
            json_serializer=self.json_serializer,
            echo=False,
        )
        _sa_engines[key] = self.eng
        return self.eng

    def dialect_is_supported(self) -> bool:
        return True

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        with self.get_engine().connect() as conn:
            yield conn

    def execute_sql(self, sql: str) -> ResultProxy:
        logger.debug("Executing SQL:")
        logger.debug(sql)
        with self.connection() as conn:
            return conn.execute(sql)

    @contextmanager
    def execute_sql_result(self, sql: str) -> Iterator[ResultProxy]:
        logger.debug("Executing SQL:")
        logger.debug(sql)
        with self.connection() as conn:
            yield conn.execute(sql)

    def ensure_table(self, name: str, schema: Schema) -> str:
        if self.exists(name):
            return name
        ddl = SchemaMapper().create_table_statement(
            schema=schema,
            dialect=self.get_engine().dialect,
            table_name=name,
        )
        self.execute_sql(ddl)
        return name

    ### StorageApi implementations ###
    def create_alias(self, from_stmt: str, alias: str):
        self.execute_sql(f"drop view if exists {alias}")
        self.execute_sql(f"create view {alias} as select * from {from_stmt}")

    def exists(self, table_name: str) -> bool:
        try:
            self.execute_sql(f"select * from {table_name} limit 0")
            return True
        except (OperationalError, ProgrammingError) as x:
            s = str(x).lower()
            if (
                "does not exist" in s or "no such" in s or "doesn't exist" in s
            ):  # TODO: HACK, implement this properly for each dialect
                return False
            raise x

    def count(self, table_name: str) -> int:
        with self.execute_sql_result(f"select count(*) from {table_name}") as res:
            row = res.fetchone()
        return row[0]

    record_count = count

    def copy(self, name: str, to_name: str):
        self.execute_sql(f"create table {to_name} as select * from {name}")

    def rename_table(self, table_name: str, new_name: str):
        self.execute_sql(f"alter table {table_name} rename to {new_name}")

    def clean_sub_sql(self, sql: str) -> str:
        return sql.strip(" ;")

    def insert_sql(self, sess: Session, name: str, sql: str, schema: Schema):
        sql = self.clean_sub_sql(sql)
        columns = "\n,".join(f.name for f in schema.fields)
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

    def create_table_from_sql(
        self,
        name: str,
        sql: str,
    ):
        sql = self.clean_sub_sql(sql)
        create_sql = f"""
        create table {name} as
        select
        *
        from (
        {sql}
        ) as __sub
        """
        self.execute_sql(create_sql)

    def get_table_schema(self, name: str) -> Schema:
        return infer_schema_from_db_table(self, name)

    def bulk_insert_records(self, name: str, records: Records, schema: Schema):
        # Create table whether or not there is anything to insert (side-effect consistency)
        # TODO: is it right to create the table? Seems useful to have an "empty" datablock, for instance.
        self.ensure_table(name, schema=schema)
        if not records:
            return
        self._bulk_insert(name, records)

    def _bulk_insert(self, table_name: str, records: Records):
        columns = conform_columns_for_insert(records)
        records = conform_records_for_insert(records, columns)
        sql = f"""
        INSERT INTO "{ table_name }" (
            "{ '","'.join(columns)}"
        ) VALUES ({','.join(['?'] * len(columns))})
        """
        conn = self.get_engine().raw_connection()
        curs = conn.cursor()
        try:
            curs.executemany(sql, records)
            conn.commit()
        finally:
            conn.close()

    def get_sqlalchemy_metadata(self):
        sa_engine = self.get_engine()
        meta = MetaData()
        meta.reflect(bind=sa_engine)
        return meta

    @classmethod
    @contextmanager
    def temp_local_database(cls) -> Iterator[str]:
        raise NotImplementedError


class DatabaseStorageApi(DatabaseApi, StorageApi):
    def __init__(
        self,
        storage: Storage,
    ):
        super().__init__(storage.url)
        self.storage = storage


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


def drop_db(url: str, database_name: str, force: bool = False):
    if url.startswith("sqlite"):
        return drop_sqlite_db(url, database_name)
    if "test" not in database_name and "tmp" not in database_name and not force:
        i = input(f"Dropping db {database_name}, are you sure? (y/N)")
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
    db_path = url[10:]
    os.remove(db_path)
