import json
import shutil
from collections import OrderedDict
from pathlib import Path
from pprint import pprint

from basis.configuration.base import dump_json
from basis.configuration.graph import GraphCfg
from basis.graph.builder import GraphManifestBuilder
from tests.cli.base import set_tmp_dir

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
        nodes=[
            {"python": "test.py"},
            {"sql": "test.sql"},
        ],
    )
    pth = Path(set_tmp_dir()) / "test_simple"
    shutil.copytree(TEST_GRAPH_DIR, pth)
    builder = GraphManifestBuilder(
        directory=pth.resolve(),
        cfg=cfg,
    )
    cfg_node = builder.build_manifest_from_config()
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
    builder = GraphManifestBuilder(
        directory=pth.resolve(),
        cfg=cfg,
    )
    cfg_node = builder.build_manifest_from_config()
    # print(dump_json(cfg_node))
    # raise
    assert cfg_node.name == cfg.name
    assert cfg_node.node_path == ""
    assert len(cfg_node.nodes) == 3
    assert len(cfg_node.interface.inputs) == 1
    assert len(cfg_node.interface.outputs) == 2
    assert len(cfg_node.interface.parameters) == 1
    sub_cfg_node = cfg_node.nodes[2]
    assert sub_cfg_node.name == "subgraph1"
    assert sub_cfg_node.node_path == "subgraph1"
    assert len(sub_cfg_node.nodes) == 1
    assert len(sub_cfg_node.interface.inputs) == 0
    assert len(sub_cfg_node.interface.outputs) == 1
    assert len(sub_cfg_node.interface.parameters) == 0
    sub_sub_cfg_node = sub_cfg_node.nodes[0]
    assert sub_sub_cfg_node.name == "test_sub"
    assert sub_sub_cfg_node.node_path == "subgraph1.test_sub"
