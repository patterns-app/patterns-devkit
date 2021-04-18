from __future__ import annotations

from dcp.utils.common import rand_str
from loguru import logger
from snapflow.core.environment import (
    Environment,
    EnvironmentConfiguration,
    SnapflowSettings,
)
from snapflow.core.graph import Graph

logger.enable("snapflow")


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
        env.add_storage("postgresql://test")
        assert len(env.storages) == 2  # added plus default local memory
        assert len(env.runtimes) == 2  # added plus default local python


def test_env_init():
    env = Environment(
        f"_test_{rand_str()}",
        metadata_storage="sqlite://",
        settings=SnapflowSettings(add_core_module=False),
    )
    env_init(env)


def test_env_config():
    cfg = EnvironmentConfiguration(
        f"_test_{rand_str()}",
        metadata_storage_url="sqlite://",
        settings=SnapflowSettings(add_core_module=False),
    )
    env = Environment.from_config(cfg)
    env_init(env)
