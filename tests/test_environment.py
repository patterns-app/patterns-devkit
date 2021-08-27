from __future__ import annotations

from basis.core.declarative.environment import BasisCfg, EnvironmentCfg
from basis.core.environment import Environment
from basis.core.persistence.block import BlockMetadata
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.common import rand_str
from loguru import logger
from sqlalchemy.sql.expression import select

from . import _test_module

# logger.enable("basis")


def env_init(env: Environment):
    # Test module / components
    with env.md_api.begin():
        env.add_schema(_test_module.TestSchema)
        assert env.get_schema("TestSchema") is _test_module.TestSchema
        assert (
            env.get_function("tests._test_module.functions.test_sql_function")
            is _test_module.functions.test_sql_function
        )
        # Test runtime / storage
        # env.add_storage("postgresql://test")
        # assert len(env.storages) == 2  # added plus default local memory
        # assert len(env.runtimes) == 2  # added plus default local python


def test_env_init():
    env = Environment(
        cfg=EnvironmentCfg(
            key=f"_test_{rand_str()}",
            metadata_storage="sqlite://",
            basis_cfg=BasisCfg(use_global_library=False),
        )
    )
    env_init(env)


def test_multi_env():
    db_url = get_tmp_sqlite_db_url()
    cfg = EnvironmentCfg(
        key=f"_test_{rand_str()}",
        metadata_storage=db_url,
        basis_cfg=BasisCfg(use_global_library=False),
    )
    env1 = Environment(cfg=cfg)
    with env1.md_api.begin():
        env1.md_api.add(BlockMetadata(realized_schema_key="Any"))
        env1.md_api.flush()
        assert env1.md_api.count(select(BlockMetadata)) == 1
    cfg = EnvironmentCfg(
        key=f"_test_{rand_str()}",
        metadata_storage=db_url,
        basis_cfg=BasisCfg(use_global_library=False),
    )
    env2 = Environment(cfg=cfg)
    with env2.md_api.begin():
        assert env2.md_api.count(select(BlockMetadata)) == 0
        env2.md_api.add(BlockMetadata(realized_schema_key="Any"))
        env2.md_api.flush()
        assert env2.md_api.count(select(BlockMetadata)) == 1
