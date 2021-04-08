from __future__ import annotations

from snapflow.core import data_block
from snapflow.core.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    get_datablock_id,
)
from snapflow.core.execution import RunContext
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, Direction, SnapLog
from snapflow.core.operators import filter, latest, operator
from snapflow.core.streams import DataBlockStream, StreamBuilder


from tests.utils import (
    TestSchema1,
    TestSchema2,
    TestSchema3,
    make_test_env,
    make_test_run_context,
    snap_generic,
    snap_t1_sink,
    snap_t1_source,
    snap_t1_to_t2,
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
    with env.md_api.begin():
        env.md_api.add(db)
        env.md_api.add(sdb)
        assert sdb.name is None
        name = sdb.get_name()
        assert len(name) > 10
        assert sdb.name == name
        strg.get_api().put(sdb.name, mdr)
        assert db.inferred_schema(env) == TestSchema1
        assert db.nominal_schema(env) == TestSchema2
        assert db.realized_schema(env) == TestSchema3
        db.compute_record_count()
        assert db.record_count == 1
