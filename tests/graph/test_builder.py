import json
import shutil
from collections import OrderedDict
from pathlib import Path
from pprint import pprint

from basis.configuration.base import dump_json
from basis.graph.builder import configured_nodes_from_yaml, graph_as_configured_nodes
from basis.node.node import NodeFunction, get_node_function
import sys

TEST_GRAPH_DIR = Path(__file__).parent / "test_graph"


def get_test_manifest():
    manifest = configured_nodes_from_yaml(TEST_GRAPH_DIR / "graph.yml")
    return manifest


def test_simple_graph_builder():
    manifest = get_test_manifest()
    nodes = manifest.nodes
    assert len(nodes) == 3 + 2
    connection_count = 0
    testpy_node = None
    subgraph1_node = None
    for node in nodes:
        pprint(node.dict(exclude_none=True))
        connection_count += len(node.flattened_connections)
        if node.node_name == "testpy":
            testpy_node = node
        if node.node_name == "subgraph1":
            subgraph1_node = node
    assert testpy_node is not None
    assert len(testpy_node.flattened_connections) == 1
    testpy_conn = testpy_node.flattened_connections[0]
    assert str(testpy_conn.input_path) == "testpy[charges]"
    assert subgraph1_node is not None
    assert len(subgraph1_node.flattened_connections) == 3
    subgraph1_conn = subgraph1_node.flattened_connections[0]
    assert str(subgraph1_conn.input_path) == "subgraph1[input1]"
    assert str(subgraph1_conn.output_path) == "subgraph1.testsub1[input_table]"


def test_find_node_function():
    sys.path.append(str(TEST_GRAPH_DIR))
    manifest = get_test_manifest()
    node = manifest.get_node("testpy")
    node_fn = get_node_function(node)
    assert isinstance(node_fn, NodeFunction)


# def test_simple_graph_builder():
#     cfg = GraphCfg(
#         name="mygraph",
#         basis={"version": "0.1"},
#         interface={
#             "inputs": [{"name": "myinput", "like": "testpy"}],
#             "outputs": OrderedDict(myoutput="test"),
#             "parameters": {"myparam": 1},
#         },
#         nodes=[
#             {"python": "test.py"},
#             {"sql": "test.sql"},
#         ],
#     )
#     pth = Path(set_tmp_dir()) / "test_simple"
#     shutil.copytree(TEST_GRAPH_DIR, pth)
#     builder = GraphManifestBuilder(
#         directory=pth.resolve(),
#         cfg=cfg,
#     )
#     cfg_node = builder.build_manifest_from_config()
#     assert cfg_node.name == cfg.name
#     assert len(cfg_node.nodes) == 2
#     assert len(cfg_node.interface.inputs) == 1
#     assert len(cfg_node.interface.outputs) == 1
#     assert len(cfg_node.interface.parameters) == 1


# def test_sub_graph_builder():
#     cfg = GraphCfg(
#         name="mygraph",
#         basis={"version": "0.1"},
#         interface={
#             "inputs": [{"name": "myinput", "like": "testpy"}],
#             "outputs": OrderedDict(myoutput="test", otheroutput="subgraph1.test_sub"),
#             "parameters": {"myparam": 1},
#         },
#         nodes=[
#             {"python": "test.py"},
#             {"sql": "test.sql"},
#             {"subgraph": "subgraph1/graph.yml"},
#         ],
#     )
#     pth = Path(set_tmp_dir()) / "test_subgraph"
#     shutil.copytree(TEST_GRAPH_DIR, pth)
#     builder = GraphManifestBuilder(
#         directory=pth.resolve(),
#         cfg=cfg,
#     )
#     cfg_node = builder.build_manifest_from_config()
#     assert cfg_node.name == cfg.name
#     assert cfg_node.node_path == ""
#     assert len(cfg_node.nodes) == 3
#     assert len(cfg_node.interface.inputs) == 1
#     assert len(cfg_node.interface.outputs) == 2
#     assert len(cfg_node.interface.parameters) == 1
#     sub_cfg_node = cfg_node.nodes[2]
#     assert sub_cfg_node.name == "subgraph1"
#     assert sub_cfg_node.node_path == "subgraph1"
#     assert len(sub_cfg_node.nodes) == 1
#     assert len(sub_cfg_node.interface.inputs) == 0
#     assert len(sub_cfg_node.interface.outputs) == 1
#     assert len(sub_cfg_node.interface.parameters) == 0
#     sub_sub_cfg_node = sub_cfg_node.nodes[0]
#     assert sub_sub_cfg_node.name == "test_sub"
#     assert sub_sub_cfg_node.node_path == "subgraph1.test_sub"

#     # Test node paths
#     n = cfg_node.find_node("subgraph1.test_sub")
#     assert n is sub_sub_cfg_node
