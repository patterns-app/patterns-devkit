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

    assert isinstance(_test_module, SnapflowModule)
    assert len(_test_module.schemas) >= 1
    assert len(_test_module.snaps) >= 2


def test_core_module():
    # These are times two because we have an entry for both `name` and `module_name.name`
    assert len(core.snaps) == 9 * 2
    assert len(core.schemas) == 3

    core.run_tests()


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
