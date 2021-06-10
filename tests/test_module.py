from __future__ import annotations

import logging

from loguru import logger
from snapflow.core.environment import Environment
from snapflow.core.function import datafunction
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.modules.core import module as core

# logger.enable("snapflow")


def test_module_init():
    from ._test_module import module as _test_module

    assert isinstance(_test_module, SnapflowModule)
    assert len(_test_module.schemas) >= 1
    assert len(_test_module.functions) >= 2


def test_core_module():
    # These are times two because we have an entry for both `name` and `namespace.name`
    assert len(core.functions) == 10
    assert len(core.schemas) == 2

    core.run_tests()


def test_default_module():
    DEFAULT_LOCAL_MODULE.library.functions = {}

    @datafunction
    def s1():
        pass

    assert len(DEFAULT_LOCAL_MODULE.library.functions) == 1
    assert DEFAULT_LOCAL_MODULE.get_function("s1") is s1

    env = Environment()
    env.add_function(s1)
    assert env.get_function("s1") is s1


if __name__ == "__main__":
    core.run_tests()
