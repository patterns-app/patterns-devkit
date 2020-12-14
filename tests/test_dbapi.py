from __future__ import annotations

import time
import warnings
from contextlib import contextmanager
from dataclasses import asdict
from typing import Any, Iterator, List, Tuple, Type

import pytest
from loguru import logger
from snapflow import Environment, graph
from snapflow.core.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    create_data_block_from_records,
    get_datablock_id,
)
from snapflow.core.data_formats import DatabaseTableFormat
from snapflow.core.node import PipeLog
from snapflow.core.storage.storage import new_local_memory_storage
from snapflow.db.api import DatabaseAPI, create_db, dispose_all, drop_db
from snapflow.db.mysql import MYSQL_SUPPORTED, MysqlDatabaseAPI
from snapflow.db.postgres import POSTGRES_SUPPORTED, PostgresDatabaseAPI
from snapflow.modules import core
from snapflow.testing.utils import get_tmp_sqlite_db_url
from snapflow.utils.common import rand_str
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm.session import Session, close_all_sessions
from tests.utils import make_test_env, sample_records


def get_sample_records_datablock(
    env: Environment, sess: Session
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    return create_data_block_from_records(
        env, sess, local_storage=new_local_memory_storage(), records=sample_records
    )


def test_conn():
    env = make_test_env()
    db = env.add_storage(get_tmp_sqlite_db_url())
    api = DatabaseAPI(env, db.url)
    api.get_engine()
    with api.connection() as conn:
        assert conn.execute("select 1").first()[0] == 1


@contextmanager
def get_sqlite_db() -> Iterator[Tuple[str, Type[DatabaseAPI]]]:
    db_url = get_tmp_sqlite_db_url("__test_snapflow_sqlite")
    yield db_url, DatabaseAPI


@contextmanager
def get_postgres_db() -> Iterator[Tuple[str, Type[DatabaseAPI]]]:
    test_db = "__test_snapflow_pg"
    url = "postgresql://localhost"
    pg_url = f"{url}/postgres"
    create_db(pg_url, test_db)
    test_url = f"{url}/{test_db}"
    try:
        yield test_url, PostgresDatabaseAPI
    finally:
        close_all_sessions()
        dispose_all()
        drop_db(pg_url, test_db)


@contextmanager
def get_mysql_db() -> Iterator[Tuple[str, Type[DatabaseAPI]]]:
    if not MYSQL_SUPPORTED:
        warnings.warn("Mysql client not found, skipping mysql-specific tests")
        return
    test_db = "__test_snapflow_mysql"
    url = "mysql://root@localhost"
    create_db(url, test_db)
    test_url = f"{url}/{test_db}"
    try:
        yield test_url, MysqlDatabaseAPI
    finally:
        close_all_sessions()
        dispose_all()
        drop_db(url, test_db)


db_generators = [get_sqlite_db]
if POSTGRES_SUPPORTED:
    db_generators.append(get_postgres_db)
else:
    warnings.warn("Postgres client not found, skipping postgres-specific tests")
if MYSQL_SUPPORTED:
    db_generators.append(get_mysql_db)
else:
    warnings.warn("Mysql client not found, skipping mysql-specific tests")

logger.enable("snapflow")


@pytest.mark.parametrize("db", db_generators)
def test_db_bulk_insert(db):
    with db() as (test_db_url, api_cls):
        env = make_test_env()
        env.add_module(core)
        db = env.add_storage(test_db_url)
        api = api_cls(env, test_db_url)
        with env.session_scope() as sess:
            b, sb = get_sample_records_datablock(env, sess)
            output_sdb = StoredDataBlockMetadata(  # type: ignore
                id=get_datablock_id(),
                data_block_id=b.id,
                data_block=b,
                data_format=DatabaseTableFormat,
                storage_url=db.url,
            )
            sess.add(output_sdb)
            api.bulk_insert_records_list(sess, output_sdb, sample_records)
            assert api.count(output_sdb.get_name(env)) == 4
            with api.connection() as conn:
                assert (
                    conn.execute(
                        f"select count(*) from {output_sdb.get_name(env)}"
                    ).first()[0]
                    == 4
                )


# TODO: test belongs somewhere else?
@pytest.mark.parametrize("db", db_generators)
def test_metadata(db):
    with db() as (test_db_url, api_cls):
        env = Environment(metadata_storage=test_db_url)

        def test_pipe():
            return [{1: 1, 2: 2}]

        g = graph()
        node = g.create_node(pipe=test_pipe)
        env.run_node(node)

        api = api_cls(env, test_db_url)
        assert api.count(DataBlockMetadata.__tablename__) == 1
        with env.session_scope() as sess:
            assert sess.query(PipeLog).count() == 1
            assert sess.query(DataBlockMetadata).count() == 1
