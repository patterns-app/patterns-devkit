import os
from pathlib import Path
from collections import OrderedDict

import shutil
import pytest
import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
from basis.configuration.graph import GraphCfg
from basis.graph.metadata import ConfiguredGraphBuilder
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir
from pydantic import ValidationError

from basis.configuration.node import GraphNodeCfg


TEST_GRAPH_DIR = Path(__file__).parent / "test_graph"


def test_simple_graph_builder():
    cfg = GraphCfg(
        name="mygraph",
        basis={"version": "0.1"},
        interface={
            "inputs": [{"name": "myinput", "like": "testpy"}],
            "outputs": OrderedDict(myoutput="test"),
            "parameters": {"myparam": 1},
        },
        nodes=[{"python": "test.py"}, {"sql": "test.sql"},],
    )
    pth = Path(set_tmp_dir())
    shutil.copy(TEST_GRAPH_DIR / "__init__.py", pth / "__init__.py")
    shutil.copy(TEST_GRAPH_DIR / "test.py", pth / "test.py")
    shutil.copy(TEST_GRAPH_DIR / "test.sql", pth / "test.sql")
    builder = ConfiguredGraphBuilder(directory=pth, cfg=cfg,)
    cfg_node = builder.build_metadata_from_config()
    assert cfg_node.name == cfg.name
    assert cfg_node.name == cfg.name

