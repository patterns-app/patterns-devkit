from pathlib import Path

from basis import GraphManifest
from basis.configuration.path import NodeId
from basis.graph.builder import graph_manifest_from_yaml
from basis.graph.configured_node import CURRENT_MANIFEST_SCHEMA_VERSION, NodeType
from tests.graph.utils import p, ostream, istream, itable, otable, assert_nodes, n


def _build_manifest(name: str) -> GraphManifest:
    pth = Path(__file__).parent / name / "graph.yml"
    return graph_manifest_from_yaml(pth)


def test_flat_graph():
    manifest = _build_manifest("flat_graph")
    assert manifest.graph_name == "Test graph"
    assert manifest.manifest_version == CURRENT_MANIFEST_SCHEMA_VERSION

    result = list(manifest.get_nodes_by_name("pass"))
    assert len(result) == 1
    node = result[0]
    assert manifest.get_node_by_id(node.id) == node

    assert list(node.local_input_edges()) == [node.local_edges[0]]
    assert list(node.resolved_input_edges()) == [node.local_edges[0]]
    assert list(node.local_output_edges()) == [node.resolved_edges[1]]
    assert list(node.resolved_output_edges()) == [node.resolved_edges[1]]

    assert_nodes(
        manifest.nodes,
        n(
            "source",
            id=NodeId.from_name("source", None),
            interface=[ostream("source_stream")],
            file_path="source.py",
            local_edges=["source:source_stream -> pass:source_stream"],
        ),
        n(
            "pass",
            description="Passthrough Desc",
            interface=[
                istream("source_stream", "in desc", "TestSchema"),
                istream("optional_stream", required=False),
                ostream("passthrough_stream", "out desc", "TestSchema2"),
                p("explicit_param", "bool", "param desc", False),
                p("plain_param"),
            ],
            file_path="passthrough.py",
            parameter_values={"plain_param": "test value"},
            local_edges=[
                "source:source_stream -> pass:source_stream",
                "pass:passthrough_stream -> mapper:input_stream",
            ],
        ),
        n(
            "mapper",
            interface=[istream("input_stream"), otable("output_table")],
            file_path="mapper/mapper.py",
            local_edges=[
                "pass:passthrough_stream -> mapper:input_stream",
                "mapper:output_table -> query:query_table",
            ],
        ),
        n(
            "query",
            interface=[
                itable("query_table"),
                otable("sink_table"),
                p("num", parameter_type="int"),
            ],
            file_path="query.sql",
            parameter_values={"num": 0},
            local_edges=[
                "mapper:output_table -> query:query_table",
                "query:sink_table -> sink:sink_table",
            ],
        ),
        n(
            "sink",
            id="ManualId",
            interface=[itable("sink_table")],
            file_path="sink.py",
            local_edges=["query:sink_table -> sink:sink_table"],
        ),
    )


def test_fanout_graph():
    manifest = _build_manifest("fanout_graph")
    assert manifest.graph_name == "fanout_graph"
    assert (
        next(manifest.get_nodes_by_name("pass1")).id
        != next(manifest.get_nodes_by_name("pass2")).id
    )

    assert_nodes(
        manifest.nodes,
        n(
            "source",
            interface=[ostream("source_stream")],
            file_path="source.py",
            local_edges=[
                "source:source_stream -> pass1:pass_in",
                "source:source_stream -> pass2:pass_in",
            ],
        ),
        n(
            "pass1",
            interface=[istream("pass_in"), ostream("pass_out")],
            file_path="pass.py",
            local_edges=[
                "source:source_stream -> pass1:pass_in",
                "pass1:pass_out -> sink:sink_stream1",
            ],
        ),
        n(
            "pass2",
            interface=[istream("pass_in"), ostream("pass_out")],
            file_path="pass.py",
            local_edges=[
                "source:source_stream -> pass2:pass_in",
                "pass2:pass_out -> sink:sink_stream2",
            ],
        ),
        n(
            "sink",
            interface=[istream("sink_stream1"), istream("sink_stream2")],
            file_path="sink.py",
            local_edges=[
                "pass1:pass_out -> sink:sink_stream1",
                "pass2:pass_out -> sink:sink_stream2",
            ],
        ),
    )


def test_nested_graph():
    manifest = _build_manifest("nested_graph")

    assert_nodes(
        manifest.nodes,
        n(
            "source",
            interface=[ostream("source_stream")],
            file_path="source.py",
            local_edges=["source:source_stream -> mid:leaf_in"],
            resolved_edges=["source:source_stream -> mid.leaf.node:leaf_in"],
        ),
        n(
            "mid",
            node_type=NodeType.Graph,
            interface=[istream("leaf_in", 'd', "S"), otable("leaf_out")],
            file_path="mid/graph.yml",
            local_edges=["source:source_stream -> mid:leaf_in",
                         "mid:leaf_out -> sink:sink_table",
                         "mid:leaf_in -> mid.leaf:leaf_in",
                         "mid.leaf:leaf_out -> mid:leaf_out"],
            resolved_edges=[],
        ),
        n(
            "sink",
            interface=[itable("sink_table")],
            file_path="sink.py",
            local_edges=["mid:leaf_out -> sink:sink_table"],
            resolved_edges=["mid.leaf.node:leaf_out -> sink:sink_table"],
        ),
        n(
            "leaf",
            node_type=NodeType.Graph,
            parent='mid',
            interface=[istream("leaf_in", 'd', "S"), otable("leaf_out")],
            file_path="mid/leaf/graph.yml",
            local_edges=["mid:leaf_in -> mid.leaf:leaf_in",
                         "mid.leaf:leaf_out -> mid:leaf_out",
                         "mid.leaf:leaf_in -> mid.leaf.node:leaf_in",
                         "mid.leaf.node:leaf_out -> mid.leaf:leaf_out"],
            resolved_edges=[],
        ),
        n(
            "node",
            interface=[istream("leaf_in", 'd', "S"), otable("leaf_out")],
            file_path="mid/leaf/node.py",
            parent='mid.leaf',
            local_edges=["mid.leaf:leaf_in -> mid.leaf.node:leaf_in",
                         "mid.leaf.node:leaf_out -> mid.leaf:leaf_out"],
            resolved_edges=["source:source_stream -> mid.leaf.node:leaf_in",
                            "mid.leaf.node:leaf_out -> sink:sink_table"],
        ),
    )
