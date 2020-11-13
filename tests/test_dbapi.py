from __future__ import annotations

from dataclasses import asdict
from typing import Any, List, Tuple

import pytest

from dags import Environment
from dags.core.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    create_data_block_from_records,
)
from dags.core.data_formats import DatabaseTableFormat
from dags.core.storage.storage import new_local_memory_storage
from dags.db.api import DatabaseAPI
from dags.modules import core
from dags.testing.utils import get_tmp_sqlite_db_url
from tests.utils import make_test_env, sample_records


def get_sample_records_datablock(
    env: Environment, sess
) -> Tuple[DataBlockMetadata, StoredDataBlockMetadata]:
    return create_data_block_from_records(
        env, sess, new_local_memory_storage(), sample_records
    )


def test_conn():
    env = make_test_env()
    db = env.add_storage(get_tmp_sqlite_db_url())
    api = DatabaseAPI(env, db.url)
    api.get_engine()
    with api.connection() as conn:
        assert conn.execute("select 1").first()[0] == 1


def test_bulk_insert():
    env = make_test_env()
    env.add_module(core)
    db = env.add_storage(get_tmp_sqlite_db_url())
    api = DatabaseAPI(env, db.url)
    sess = env.get_new_metadata_session()
    b, sb = get_sample_records_datablock(env, sess)
    output_sdb = StoredDataBlockMetadata(  # type: ignore
        data_block=b, data_format=DatabaseTableFormat, storage_url=db.url,
    )
    sess.add(output_sdb)
    sess.commit()
    api.bulk_insert_records_list(output_sdb, sample_records)
    # assert api.count(output_sdb.get_name(env)) == 4
    with api.connection() as conn:
        assert (
            conn.execute(f"select count(*) from {output_sdb.get_name(env)}").first()[0]
            == 4
        )
