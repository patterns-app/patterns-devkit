from __future__ import annotations

from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.common import rand_str
from loguru import logger
from snapflow.core.data_block import DataBlockMetadata
from snapflow.core.declarative.dataspace import DataspaceCfg, SnapflowCfg
from snapflow.core.environment import Environment
from sqlalchemy.sql.expression import select

# logger.enable("snapflow")


def env_init(env: Environment):
    from ._test_module import module as _test_module

    # Test module / components
    with env.md_api.begin():
        assert len(env.get_module_order()) == 1
        env.add_module(_test_module)
        assert env.get_module_order() == [
            env.get_local_module().namespace,
            _test_module.namespace,
        ]
        assert env.get_schema("TestSchema") is _test_module.schemas.TestSchema
        assert (
            env.get_function("test_sql_function")
            is _test_module.functions.test_sql_function
        )
        # Test runtime / storage
        # env.add_storage("postgresql://test")
        # assert len(env.storages) == 2  # added plus default local memory
        # assert len(env.runtimes) == 2  # added plus default local python


def test_env_init():
    env = Environment(
        dataspace=DataspaceCfg(
            key=f"_test_{rand_str()}",
            metadata_storage="sqlite://",
            snapflow=SnapflowCfg(use_global_library=False),
        )
    )
    env_init(env)


def test_multi_env():
    db_url = get_tmp_sqlite_db_url()
    cfg = DataspaceCfg(
        key=f"_test_{rand_str()}",
        metadata_storage=db_url,
        snapflow=SnapflowCfg(use_global_library=False),
    )
    env1 = Environment(dataspace=cfg)
    with env1.md_api.begin():
        env1.md_api.add(DataBlockMetadata(realized_schema_key="Any"))
        env1.md_api.flush()
        assert env1.md_api.count(select(DataBlockMetadata)) == 1
    cfg = DataspaceCfg(
        key=f"_test_{rand_str()}",
        metadata_storage=db_url,
        snapflow=SnapflowCfg(use_global_library=False),
    )
    env2 = Environment(dataspace=cfg)
    with env2.md_api.begin():
        assert env2.md_api.count(select(DataBlockMetadata)) == 0
        env2.md_api.add(DataBlockMetadata(realized_schema_key="Any"))
        env2.md_api.flush()
        assert env2.md_api.count(select(DataBlockMetadata)) == 1
