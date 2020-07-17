from __future__ import annotations

from basis.core.environment import Environment


def test_env_init():
    from . import _test_module

    env = Environment("_test", metadata_storage="sqlite://", initial_modules=[])
    assert len(env.get_module_order()) == 1
    assert len(env.all_added_nodes()) == 0
    assert len(env.all_flattened_nodes()) == 0
    env.add_module(_test_module)
    assert env.get_module_order() == [env.get_local_module().name, _test_module.name]
    assert env.get_otype("TestType") is _test_module.otypes.TestType
    env.add_node("n1", _test_module.functions.test_sql, inputs="doesntexist")
    assert len(env.all_added_nodes()) == 1
    assert len(env.all_flattened_nodes()) == 1
    n2 = env.add_node("n2", "test_sql")
    assert env.get_node("n2") is n2
    assert env.get_function("test_sql") is _test_module.functions.test_sql
    env.add_storage("postgres://test")
    assert len(env.storages) == 1
    assert len(env.runtimes) == 2

    assert (
        env.get_external_resource("TestExtResource")
        is list(_test_module.external_resources)[0]
    )
