from __future__ import annotations

from dcp.data_format.formats.memory.records import RecordsFormat
from snapflow.core.persistence.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    get_datablock_id,
    get_stored_datablock_id,
)
from tests.utils import TestSchema1, TestSchema2, TestSchema3, make_test_env


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
    sdb = StoredDataBlockMetadata(
        id=get_stored_datablock_id(),
        name="_test",
        data_block_id=db.id,
        data_block=db,
        storage_url=strg.url,
        data_format=RecordsFormat,
    )
    with env.md_api.begin():
        env.md_api.add(db)
        env.md_api.add(sdb)
        assert sdb.name == "_test"
        strg.get_api().put(sdb.name, records)
        assert env.get_schema(db.inferred_schema_key) == TestSchema1
        assert env.get_schema(db.nominal_schema_key) == TestSchema2
        assert env.get_schema(db.realized_schema_key) == TestSchema3
        db.compute_record_count()
        assert db.record_count == 1
