from __future__ import annotations

from dags.core.module import DagsModule
from dags.modules import core


def test_module_init():
    from . import _test_module

    assert isinstance(_test_module, DagsModule)
    assert len(_test_module.schemas) >= 1
    assert len(_test_module.pipes) >= 2


if __name__ == "__main__":
    core.run_tests()
