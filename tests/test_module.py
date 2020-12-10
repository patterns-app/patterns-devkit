from __future__ import annotations

import logging

from loguru import logger
from snapflow.core.module import SnapflowModule
from snapflow.modules import core


def test_module_init():
    from . import _test_module

    assert isinstance(_test_module, SnapflowModule)
    assert len(_test_module.schemas) >= 1
    assert len(_test_module.pipes) >= 2


def test_core_module():
    core.run_tests()


if __name__ == "__main__":
    core.run_tests()
