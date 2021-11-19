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

    assert list(node.input_edges()) == [node.absolute_edges[0]]
    assert list(node.output_edges()) == [node.absolute_edges[1]]

    assert_nodes(
        manifest.nodes,
        n(
            "source",
            id=NodeId.from_name("source", None),
            interface=[ostream("source_stream")],
            node_depth=0,
            file_path="source.py",
            absolute_edges=["source:source_stream -> pass:source_stream"],
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
            node_depth=0,
            file_path="passthrough.py",
            parameter_values={"plain_param": "test value"},
            absolute_edges=[
                "source:source_stream -> pass:source_stream",
                "pass:passthrough_stream -> mapper:input_stream",
            ],
        ),
        n(
            "mapper",
            interface=[istream("input_stream"), otable("output_table")],
            file_path="mapper/mapper.py",
            declared_edges=[
                "passthrough_stream -> input_stream",
                "output_table -> query_table",
            ],
            absolute_edges=[
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
            absolute_edges=[
                "mapper:output_table -> query:query_table",
                "query:sink_table -> sink:sink_table",
            ],
        ),
        n(
            "sink",
            id="ManualId",
            interface=[itable("sink_table")],
            file_path="sink.py",
            absolute_edges=["query:sink_table -> sink:sink_table"],
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
            absolute_edges=[
                "source:source_stream -> pass1:pass_in",
                "source:source_stream -> pass2:pass_in",
            ],
        ),
        n(
            "pass1",
            interface=[istream("pass_in"), ostream("pass_out")],
            file_path="pass.py",
            declared_edges=["source_stream -> pass_in", "pass_out -> sink_stream1"],
            absolute_edges=[
                "source:source_stream -> pass1:pass_in",
                "pass1:pass_out -> sink:sink_stream1",
            ],
        ),
        n(
            "pass2",
            node_type=NodeType.Node,
            interface=[istream("pass_in"), ostream("pass_out")],
            file_path="pass.py",
            declared_edges=["source_stream -> pass_in", "pass_out -> sink_stream2"],
            absolute_edges=[
                "source:source_stream -> pass2:pass_in",
                "pass2:pass_out -> sink:sink_stream2",
            ],
        ),
        n(
            "sink",
            interface=[istream("sink_stream1"), istream("sink_stream2")],
            file_path="sink.py",
            absolute_edges=[
                "pass1:pass_out -> sink:sink_stream1",
                "pass2:pass_out -> sink:sink_stream2",
            ],
        ),
    )


def test_fanout_nested_graph():
    manifest = _build_manifest('fanout_nested_graph')
    assert manifest.graph_name == 'fanout_nested_graph'

    assert_nodes(manifest.nodes,
                 n('source'),
                 n('pass',
                   interface=[istream('pass_in'), ostream('pass_out')],
                   file_path='pass.py',
                   declared_edges=['source_stream -> pass_in'],
                   absolute_edges=[]  # TODO
                   ),
                 n('sub1',
                   node_type=NodeType.Graph,
                   interface=[istream('subgraph_in'), ostream('subgraph_out')],
                   file_path='subgraph/graph.yml',
                   declared_edges=['pass_out -> subgraph_in', 'subgraph_out -> subgraph_1_out'],
                   absolute_edges=['pass:pass_out -> sub1:subgraph_in',
                                   'sub1:subgraph_out -> sub2:subgraph_in',
                                   'sub1:subgraph_in -> sub1.pass:pass_in',
                                   'sub1.subgraph:subgraph_out -> sub1:subgraph_out']
                   ),

                 n(
                     name='sub2',
                     node_type=NodeType.Graph,
                     interface=[istream('subgraph_in'), ostream('subgraph_out')],
                     file_path='subgraph/graph.yml',
                     declared_edges=['subgraph_1_out -> subgraph_in', 'subgraph_out -> sink_stream'],
                     absolute_edges=['sub1:subgraph_out -> sub2:subgraph_in',
                                     'sub2:subgraph_out -> sink:sink_stream',
                                     'sub2:subgraph_in -> sub2.pass:pass_in',
                                     'sub2.subgraph:subgraph_out -> sub2:subgraph_out']
                 ),

                 n(
                     name='sink',
                     node_type=NodeType.Node,
                     interface=[istream('sink_stream')],
                     file_path='sink.py',
                     declared_edges=[],
                     absolute_edges=['sub2:subgraph_out -> sink:sink_stream',
                                     'sub2.subgraph.pass:subgraph_out -> sink:sink_stream']
                 ),

                 # sub1
                 n(
                     name='pass',
                     node_type=NodeType.Node,
                     parent='sub1',
                     interface=[istream('pass_in'), ostream('pass_out')],
                     file_path='subgraph/pass.py',
                     declared_edges=['subgraph_in -> pass_in', 'pass_out -> subgraph_in'],
                     absolute_edges=['pass:pass_out -> sub1.pass:pass_in',
                                     'sub1:subgraph_in -> sub1.pass:pass_in',
                                     'sub1.pass:pass_in -> sub1.subgraph:subgraph_in',
                                     'sub1.pass:pass_out -> sub1.subgraph.pass:pass_in']
                 ),

                 n(
                     name='subgraph',
                     node_type=NodeType.Graph,
                     parent='sub1',
                     interface=[istream('subgraph_in'), ostream('subgraph_out')],
                     file_path='subgraph/subgraph/graph.yml',
                     declared_edges=[],
                     absolute_edges=['sub1.pass:pass_in -> sub1.subgraph:subgraph_in',
                                     'sub1.subgraph:subgraph_out -> sub1:subgraph_out']
                 ),

                 # sub1.subgraph
                 n(
                     name='pass',
                     node_type=NodeType.Node,
                     parent='sub1.subgraph',
                     interface=[istream('pass_in'), ostream('pass_out')],
                     file_path='subgraph/subgraph/pass.py',
                     declared_edges=['subgraph_in -> pass_in', 'pass_out -> subgraph_out'],
                     absolute_edges=['sub1.pass:pass_out -> sub1.subgraph.pass:pass_in',
                                     'sub1.subgraph:subgraph_in -> sub1.subgraph.pass:pass_in',
                                     'sub1.subgraph.pass:subgraph_out -> sub1.subgraph:subgraph_out',
                                     'sub1.subgraph.pass:subgraph_out -> sub2.pass:pass_in']
                 ),

                 # sub2
                 n(
                     name='pass',
                     node_type=NodeType.Node,
                     parent='sub2',
                     interface=[istream('pass_in'), ostream('pass_out')],
                     file_path='subgraph/pass.py',
                     declared_edges=['subgraph_in -> pass_in', 'pass_out -> subgraph_in'],
                     absolute_edges=['sub1.subgraph.pass:subgraph_out -> sub2.pass:pass_in',
                                     'sub2:subgraph_in -> sub2.pass:pass_in',
                                     'sub2.pass:pass_in -> sub2.subgraph:subgraph_in',
                                     'sub2.pass:pass_out -> sub2.subgraph.pass:pass_in']
                 ),

                 n(
                     name='subgraph',
                     node_type=NodeType.Graph,
                     parent='sub2',
                     interface=[istream('subgraph_in'), ostream('subgraph_out')],
                     file_path='subgraph/subgraph/graph.yml',
                     declared_edges=[],
                     absolute_edges=['sub2.pass:pass_in -> sub2.subgraph:subgraph_in',
                                     'sub2.subgraph:subgraph_out -> sub2:subgraph_out']
                 ),

                 # sub2.subgraph
                 n(
                     name='pass',
                     node_type=NodeType.Node,
                     parent='sub2.subgraph',
                     interface=[istream('pass_in'), ostream('pass_out')],
                     file_path='subgraph/subgraph/pass.py',
                     declared_edges=['subgraph_in -> pass_in', 'pass_out -> subgraph_out'],
                     absolute_edges=['sub2.pass:pass_out -> sub2.subgraph.pass:pass_in',
                                     'sub2.subgraph:subgraph_in -> sub2.subgraph.pass:pass_in',
                                     'sub2.subgraph.pass:subgraph_out -> sub2.subgraph:subgraph_out',
                                     'sub2.subgraph.pass:subgraph_out -> sink:sink_stream']
                 )
                 )
