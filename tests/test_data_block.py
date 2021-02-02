from __future__ import annotations

from snapflow.core import data_block
from snapflow.core.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    get_datablock_id,
)
from snapflow.core.execution import RunContext
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, Direction, PipeLog
from snapflow.core.operators import filter, latest, operator
from snapflow.core.streams import DataBlockStream, StreamBuilder
from snapflow.storage.data_formats.database_table import DatabaseTableFormat
from snapflow.storage.data_formats.records import RecordsFormat
from snapflow.storage.data_records import as_records
from snapflow.storage.db.utils import get_tmp_sqlite_db_url
from tests.utils import (
    TestSchema1,
    TestSchema2,
    TestSchema3,
    make_test_env,
    make_test_run_context,
    pipe_generic,
    pipe_t1_sink,
    pipe_t1_source,
    pipe_t1_to_t2,
)


def test_data_block_methods():
    env = make_test_env()
    db = DataBlockMetadata(
        id=get_datablock_id(),
        inferred_schema_key="_test.TestSchema1",
        nominal_schema_key="_test.TestSchema2",
        realized_schema_key="_test.TestSchema3",
    )
    strg = env.get_default_local_python_storage()
    records = [{"a": 1}]
    mdr = as_records(records)
    sdb = StoredDataBlockMetadata(
        id=get_datablock_id(),
        data_block_id=db.id,
        data_block=db,
        storage_url=strg.url,
        data_format=RecordsFormat,
    )
    with env.session_scope() as sess:
        sess.add(db)
        sess.add(sdb)
        assert sdb.name is None
        name = sdb.get_name()
        assert len(name) > 10
        assert sdb.name == name
        strg.get_api().put(sdb.name, mdr)
        assert db.inferred_schema(env, sess) == TestSchema1
        assert db.nominal_schema(env, sess) == TestSchema2
        assert db.realized_schema(env, sess) == TestSchema3
        db.compute_record_count()
        assert db.record_count == 1
