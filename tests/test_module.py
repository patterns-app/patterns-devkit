from __future__ import annotations

import logging

from loguru import logger
from snapflow.core.environment import Environment
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.core.snap import Snap
from snapflow.modules import core

logger.enable("snapflow")


def test_module_init():
    from . import _test_module

    assert isinstance(_test_module.module, SnapflowModule)
    assert len(_test_module.all_schemas) >= 1
    assert len(_test_module.all_snaps) >= 2


def test_core_module():
    # These are times two because we have an entry for both `name` and `namespace.name`
    assert len(core.all_snaps) == 8
    assert len(core.all_schemas) == 2

    core.module.run_tests()


def test_default_module():
    DEFAULT_LOCAL_MODULE.library.snaps = {}

    @Snap
    def s1():
        pass

    assert len(DEFAULT_LOCAL_MODULE.library.snaps) == 1
    assert DEFAULT_LOCAL_MODULE.get_snap("s1") is s1

    env = Environment()
    env.add_snap(s1)
    assert env.get_snap("s1") is s1


if __name__ == "__main__":
    core.run_tests()
