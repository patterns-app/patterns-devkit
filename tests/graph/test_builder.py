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
    pth = Path(set_tmp_dir()) / "test_simple"
    shutil.copytree(TEST_GRAPH_DIR, pth)
    builder = ConfiguredGraphBuilder(directory=pth, cfg=cfg,)
    cfg_node = builder.build_metadata_from_config()
    assert cfg_node.name == cfg.name
    assert len(cfg_node.nodes) == 2
    assert len(cfg_node.interface.inputs) == 1
    assert len(cfg_node.interface.outputs) == 1
    assert len(cfg_node.interface.parameters) == 1


def test_sub_graph_builder():
    cfg = GraphCfg(
        name="mygraph",
        basis={"version": "0.1"},
        interface={
            "inputs": [{"name": "myinput", "like": "testpy"}],
            "outputs": OrderedDict(myoutput="test", otheroutput="subgraph1.test_sub"),
            "parameters": {"myparam": 1},
        },
        nodes=[
            {"python": "test.py"},
            {"sql": "test.sql"},
            {"subgraph": "subgraph1/graph.yml"},
        ],
    )
    pth = Path(set_tmp_dir()) / "test_subgraph"
    shutil.copytree(TEST_GRAPH_DIR, pth)
    builder = ConfiguredGraphBuilder(directory=pth, cfg=cfg,)
    cfg_node = builder.build_metadata_from_config()
    assert cfg_node.name == cfg.name
    assert len(cfg_node.nodes) == 3
    assert len(cfg_node.interface.inputs) == 1
    assert len(cfg_node.interface.outputs) == 2
    assert len(cfg_node.interface.parameters) == 1
    sub_cfg_node = cfg_node.nodes[2]
    assert len(sub_cfg_node.nodes) == 1
    assert len(sub_cfg_node.interface.inputs) == 0
    assert len(sub_cfg_node.interface.outputs) == 1
    assert len(sub_cfg_node.interface.parameters) == 0
