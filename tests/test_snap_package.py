from __future__ import annotations

import logging
from pprint import pprint
from pathlib import Path
from snapflow.core.execution.execution import SnapContext
from snapflow.core.snap_interface import DeclaredSnapInterface
from loguru import logger
from snapflow.core.environment import Environment
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.core.snap_package import SnapPackage
from snapflow.modules import core
from snapflow import _Snap, Snap

logger.enable("snapflow")


def test_from_path():
    pth = Path(__file__).resolve().parents[0] / "_test_module/snaps/test_snap"
    mfsnap = SnapPackage.from_path(str(pth))
    snap = mfsnap.snap
    assert snap.name == mfsnap.name
    i: DeclaredSnapInterface = snap.get_interface()
    assert not i.inputs
    assert i.context is not None
    readme = mfsnap.load_readme()
    assert len(readme) > 5
    assert len(mfsnap.tests) == 2
    assert len(mfsnap.tests[0]["inputs"]) == 1


def test_from_snap():
    @Snap
    def snap1(ctx: SnapContext):
        pass

    mfsnap = SnapPackage.from_snap(snap1)
    assert mfsnap.root_path == Path(__file__).resolve()
    assert snap1.name == mfsnap.name
    snap = mfsnap.snap
    assert snap.name == "snap1"
    i: DeclaredSnapInterface = snap.get_interface()
    assert not i.inputs
    assert i.context is not None


def test_sql_snap():
    pth = Path(__file__).resolve().parents[0] / "_test_module/snaps/test_sql_snap"
    mfsnap = SnapPackage.from_path(str(pth))
    snap = mfsnap.snap
    assert snap.name == mfsnap.name
    i: DeclaredSnapInterface = snap.get_interface()
    assert len(i.inputs) == 1
    assert i.context is not None
    readme = mfsnap.load_readme()
    assert len(readme) > 5


# def test_from_module():
#     from . import _test_module

# snaps = SnapPackage.all_from_root_module(_test_module)
# print(snaps)
# assert len(snaps) == 2
