from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any, List, Tuple, Type

import pytest
from loguru import logger

from snapflow import Environment
from snapflow.core.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    create_data_block_from_records,
)
from snapflow.core.data_formats import DatabaseTableFormat
from snapflow.core.storage.storage import new_local_memory_storage
from snapflow.db.api import DatabaseAPI, create_db, dispose_all, drop_db
from snapflow.db.mysql import MYSQL_SUPPORTED, MysqlDatabaseAPI
from snapflow.db.postgres import POSTGRES_SUPPORTED, PostgresDatabaseAPI
from snapflow.modules import core
from snapflow.testing.utils import get_tmp_sqlite_db_url
from snapflow.utils.common import rand_str
from tests.utils import make_test_env, sample_records


def get_sample_records_datablock(
    env: Environment, sess
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    return create_data_block_from_records(
        env, local_storage=new_local_memory_storage(), records=sample_records
    )


def test_conn():
    env = make_test_env()
    db = env.add_storage(get_tmp_sqlite_db_url())
    api = DatabaseAPI(env, db.url)
    api.get_engine()
    with api.connection() as conn:
        assert conn.execute("select 1").first()[0] == 1


def _test_db_bulk_insert(test_db_url, api_cls: Type[DatabaseAPI]):
    env = make_test_env()
    env.add_module(core)
    db = env.add_storage(test_db_url)
    api = api_cls(env, test_db_url)
    b, sb = get_sample_records_datablock(env, env.session)
    output_sdb = StoredDataBlockMetadata(  # type: ignore
        data_block=b,
        data_format=DatabaseTableFormat,
        storage_url=db.url,
    )
    env.session.add(output_sdb)
    env.session.commit()
    api.bulk_insert_records_list(output_sdb, sample_records)
    assert api.count(output_sdb.get_name(env)) == 4
    with api.connection() as conn:
        assert (
            conn.execute(f"select count(*) from {output_sdb.get_name(env)}").first()[0]
            == 4
        )


def test_sqlite_bulk_insert():
    db_url = get_tmp_sqlite_db_url("__test_dags_sqlite")
    _test_db_bulk_insert(db_url, DatabaseAPI)


def test_postgres_bulk_insert():
    if not POSTGRES_SUPPORTED:
        logger.warning("Postgres client not found, skipping postgres-specific tests")
        return
    test_db = "__test_dags_pg"
    url = "postgres://postgres@localhost:5432"
    pg_url = f"{url}/postgres"
    create_db(pg_url, test_db)
    test_url = f"{url}/{test_db}"
    try:
        _test_db_bulk_insert(test_url, PostgresDatabaseAPI)
    finally:
        dispose_all()
        drop_db(pg_url, test_db)


def test_mysql_bulk_insert():
    if not MYSQL_SUPPORTED:
        logger.warning("Mysql client not found, skipping mysql-specific tests")
        return
    test_db = "__test_dags_pg"
    url = "mysql://mysql@localhost:3306"
    create_db(url, test_db)
    test_url = f"{url}/{test_db}"
    try:
        _test_db_bulk_insert(test_url, MysqlDatabaseAPI)
    finally:
        dispose_all()
        drop_db(url, test_db)
