from __future__ import annotations

from dags.core.environment import Environment
from dags.core.graph import Graph


def test_env_init():
    from . import _test_module

    # Test module / components
    env = Environment("_test", metadata_storage="sqlite://", initial_modules=[])
    assert len(env.get_module_order()) == 1
    env.add_module(_test_module)
    assert env.get_module_order() == [env.get_local_module().key, _test_module.key]
    assert env.get_schema("TestSchema") is _test_module.schemas.TestSchema
    assert env.get_pipe("test_sql") is _test_module.pipes.test_sql
    # Test runtime / storage
    env.add_storage("postgres://test")
    assert len(env.storages) == 2  # added plus default local memory
    assert len(env.runtimes) == 2  # added plus default local python
