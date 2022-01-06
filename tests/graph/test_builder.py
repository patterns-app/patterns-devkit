from pathlib import Path

import pytest
from pydantic import ValidationError
from basis.configuration.path import NodeId
from basis.graph.builder import graph_manifest_from_yaml, GraphManifest, GraphError
from basis.graph.configured_node import CURRENT_MANIFEST_SCHEMA_VERSION, NodeType
from tests.graph.utils import (
    p,
    ostream,
    istream,
    itable,
    otable,
    assert_nodes,
    n,
    read_manifest,
    setup_manifest,
    s,
)


def test_flat_graph():
    manifest = read_manifest("flat_graph")
    assert manifest.graph_name == "Test graph"
    assert manifest.manifest_version == CURRENT_MANIFEST_SCHEMA_VERSION

    assert_nodes(
        manifest,
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
                s("state_param"),
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

    result = list(manifest.get_nodes_by_name("pass"))
    assert len(result) == 1
    node = result[0]
    assert manifest.get_node_by_id(node.id) == node

    assert list(node.local_input_edges()) == [node.local_edges[0]]
    assert list(node.resolved_input_edges()) == [node.local_edges[0]]
    assert list(node.local_output_edges()) == [node.resolved_edges[1]]
    assert list(node.resolved_output_edges()) == [node.resolved_edges[1]]


def test_fanout_graph():
    manifest = read_manifest("fanout_graph")
    assert manifest.graph_name == "fanout_graph"
    assert (
        next(manifest.get_nodes_by_name("pass1")).id
        != next(manifest.get_nodes_by_name("pass2")).id
    )

    assert_nodes(
        manifest,
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
    manifest = read_manifest("nested_graph")

    assert_nodes(
        manifest,
        n(
            "source",
            interface=[ostream("source_stream")],
            file_path="source.py",
            local_edges=["source:source_stream -> mid:mid_in"],
            resolved_edges=["source:source_stream -> mid.leaf.node:node_in"],
        ),
        n(
            "mid",
            node_type=NodeType.Graph,
            interface=[istream("mid_in", "d", "S"), otable("mid_out")],
            file_path="mid/graph.yml",
            local_edges=[
                "source:source_stream -> mid:mid_in",
                "mid:mid_out -> sink:sink_table",
                "mid:mid_in -> mid.leaf:leaf_in",
                "mid.leaf:leaf_out -> mid:mid_out",
            ],
            resolved_edges=[],
        ),
        n(
            "sink",
            interface=[itable("sink_table")],
            file_path="sink.py",
            local_edges=["mid:mid_out -> sink:sink_table"],
            resolved_edges=["mid.leaf.node:node_out -> sink:sink_table"],
        ),
        n(
            "leaf",
            node_type=NodeType.Graph,
            parent="mid",
            interface=[istream("leaf_in", "d", "S"), otable("leaf_out")],
            file_path="mid/leaf/graph.yml",
            local_edges=[
                "mid:mid_in -> mid.leaf:leaf_in",
                "mid.leaf:leaf_out -> mid:mid_out",
                "mid.leaf:leaf_in -> mid.leaf.node:node_in",
                "mid.leaf.node:node_out -> mid.leaf:leaf_out",
            ],
            resolved_edges=[],
        ),
        n(
            "node",
            interface=[istream("node_in", "d", "S"), otable("node_out")],
            file_path="mid/leaf/node.py",
            parent="mid.leaf",
            local_edges=[
                "mid.leaf:leaf_in -> mid.leaf.node:node_in",
                "mid.leaf.node:node_out -> mid.leaf:leaf_out",
            ],
            resolved_edges=[
                "source:source_stream -> mid.leaf.node:node_in",
                "mid.leaf.node:node_out -> sink:sink_table",
            ],
        ),
    )


def test_fanout_subgraph():
    manifest = read_manifest("fanout_subgraph")
    assert_nodes(
        manifest,
        n(
            "source",
            interface=[ostream("source_stream")],
            file_path="source.py",
            local_edges=["source:source_stream -> sub:sub_in"],
            resolved_edges=["source:source_stream -> sub.node:node_in"],
        ),
        n(
            "sub",
            node_type=NodeType.Graph,
            interface=[istream("sub_in", "d", "S"), ostream("sub_out")],
            file_path="sub/graph.yml",
            local_edges=[
                "source:source_stream -> sub:sub_in",
                "sub:sub_out -> sub2:sub_in",
                "sub:sub_in -> sub.node:node_in",
                "sub.node:node_out -> sub:sub_out",
            ],
            resolved_edges=[],
        ),
        n(
            "sub2",
            node_type=NodeType.Graph,
            interface=[istream("sub_in", "d", "S"), ostream("sub_out")],
            file_path="sub/graph.yml",
            local_edges=[
                "sub:sub_out -> sub2:sub_in",
                "sub2:sub_out -> sink:sink_table",
                "sub2:sub_in -> sub2.node:node_in",
                "sub2.node:node_out -> sub2:sub_out",
            ],
            resolved_edges=[],
        ),
        n(
            "node",
            interface=[istream("node_in", "d", "S"), ostream("node_out")],
            file_path="sub/node.py",
            parent="sub",
            local_edges=[
                "sub:sub_in -> sub.node:node_in",
                "sub.node:node_out -> sub:sub_out",
            ],
            resolved_edges=[
                "source:source_stream -> sub.node:node_in",
                "sub.node:node_out -> sub2.node:node_in",
            ],
        ),
        n(
            "node",
            interface=[istream("node_in", "d", "S"), ostream("node_out")],
            file_path="sub/node.py",
            parent="sub2",
            local_edges=[
                "sub2:sub_in -> sub2.node:node_in",
                "sub2.node:node_out -> sub2:sub_out",
            ],
            resolved_edges=[
                "sub.node:node_out -> sub2.node:node_in",
                "sub2.node:node_out -> sink:sink_table",
            ],
        ),
        n(
            "sink",
            interface=[istream("sink_table")],
            file_path="sink.py",
            local_edges=["sub2:sub_out -> sink:sink_table"],
            resolved_edges=["sub2.node:node_out -> sink:sink_table"],
        ),
    )


def test_empty_graph(tmp_path: Path):
    manifest = setup_manifest(tmp_path, {"graph.yml": ""})
    assert_nodes(manifest)


def test_exposing_implicit_ports(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                nodes:
                  - node_file: source.py
                  - node_file: sink.py
                  - node_file: sub/graph.yml""",
            "sub/graph.yml": """
                exposes:
                  inputs:
                    - to_graph
                  outputs:
                    - from_graph
                nodes:
                  - node_file: node.py""",
            "source.py": "to_graph=OutputStream",
            "sink.py": "from_graph=InputStream",
            "sub/node.py": "to_graph=InputStream, from_graph=OutputStream",
        },
    )

    assert_nodes(
        manifest,
        n("source", resolved_edges=["source:to_graph -> sub.node:to_graph"]),
        n("sink", resolved_edges=["sub.node:from_graph -> sink:from_graph"]),
        n(
            "node",
            parent="sub",
            resolved_edges=[
                "source:to_graph -> sub.node:to_graph",
                "sub.node:from_graph -> sink:from_graph",
            ],
        ),
        n("sub", node_type=NodeType.Graph),
    )


def test_unconnected_outputs(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                nodes:
                  - node_file: source.py
                  - node_file: sub/graph.yml""",
            "sub/graph.yml": """
                exposes:
                  outputs:
                    - from_graph
                nodes:
                  - node_file: node.py""",
            "source.py": "source_stream=OutputStream",
            "sub/node.py": "from_graph=OutputTable, node_out=OutputStream",
        },
    )

    assert_nodes(
        manifest,
        n("source", interface=[ostream("source_stream")]),
        n("sub", node_type=NodeType.Graph, interface=[otable("from_graph")]),
        n("node", parent="sub", interface=[otable("from_graph"), ostream("node_out")]),
    )


def test_stream_to_table(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                nodes:
                  - node_file: source.py
                  - node_file: node.py
                    inputs:
                      - source_stream -> stream_as_table""",
            "source.py": "source_stream=OutputStream",
            "node.py": "stream_as_table=InputTable, node_out=OutputTable",
        },
    )

    assert_nodes(
        manifest,
        n(
            "source",
            interface=[ostream("source_stream")],
            local_edges=["source:source_stream -> node:stream_as_table"],
        ),
        n(
            "node",
            interface=[itable("stream_as_table"), otable("node_out")],
            local_edges=["source:source_stream -> node:stream_as_table"],
        ),
    )


def test_webhooks(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                nodes:
                  - webhook: hook1
                  - webhook: hook2
                    id: ab234567
                    name: myhook
                    description: my hook
                  - node_file: sink.py""",
            "sink.py": "hook2=InputStream",
        },
    )

    assert_nodes(
        manifest,
        n("hook1", interface=[ostream("hook1")], node_type=NodeType.Webhook),
        n(
            "myhook",
            interface=[ostream("hook2")],
            id="ab234567",
            description="my hook",
            node_type=NodeType.Webhook,
        ),
        n(
            "sink",
            interface=[istream("hook2")],
            local_edges=["myhook:hook2 -> sink:hook2"],
        ),
    )


def test_chart(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                   nodes:
                     - node_file: node.py
                     - node_file: chart1.json
                       chart_input: node_out
                       id: ab234567
                       name: mychart
                       description: my chart
                     - node_file: chart2.json
                       chart_input: node_out
                     """,
            "node.py": "node_out=OutputTable",
            "chart1.json": "{}",  # chart file content doesn't matter
            "chart2.json": "{}",
        },
    )

    assert_nodes(
        manifest,
        n(
            "node",
            interface=[otable("node_out")],
            local_edges=[
                "node:node_out -> mychart:node_out",
                "node:node_out -> chart2:node_out",
            ],
        ),
        n(
            "mychart",
            interface=[itable("node_out")],
            id="ab234567",
            description="my chart",
            file_path="chart1.json",
            node_type=NodeType.Chart,
            local_edges=["node:node_out -> mychart:node_out"],
        ),
        n(
            "chart2",
            interface=[itable("node_out")],
            file_path="chart2.json",
            node_type=NodeType.Chart,
            local_edges=["node:node_out -> chart2:node_out"],
        ),
    )


def test_err_unconnected_implicit_input(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
            nodes:
              - node_file: source.py
              - node_file: sink.py""",
            "source.py": "output=OutputStream",
            "sink.py": "input=InputStream",
        },
    )

    assert_nodes(
        manifest, n("source"), n("sink", errors=['Cannot find output named "input"']),
    )


def test_err_unconnected_explicit_input(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
            nodes:
              - node_file: source.py
                outputs:
                  - port -> nosource
              - node_file: sink.py
                inputs:
                  - nosink -> port""",
            "source.py": "port=OutputStream",
            "sink.py": "port=InputStream",
        },
    )

    assert_nodes(
        manifest, n("source"), n("sink", errors=['Cannot find output named "nosink"']),
    )


def test_err_table_to_stream(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
            nodes:
              - node_file: source.py
              - node_file: sink.py""",
            "source.py": "erport=OutputTable",
            "sink.py": "erport=InputStream",
        },
    )

    assert_nodes(
        manifest,
        n("source"),
        n(
            "sink",
            errors=["Cannot connect erport: input is a table, but output is a stream"],
        ),
    )


def test_err_invalid_yml_name(tmp_path: Path):
    path = tmp_path / "invalid.yml"
    (path).write_text("")
    with pytest.raises(ValueError):
        graph_manifest_from_yaml(path)


def test_err_invalid_yml_ext(tmp_path: Path):
    path = tmp_path / "graph.yaml"
    (path).write_text("")
    with pytest.raises(ValueError):
        graph_manifest_from_yaml(path)


def test_err_invalid_nested_yml_name(tmp_path: Path):
    path = tmp_path / "invalid.yml"
    (path).write_text("")
    with pytest.raises(ValueError):
        setup_manifest(
            tmp_path,
            {
                "graph.yml": """
                    nodes:
                      - node_file: sub/invalid.yml""",
                "sub/invalid.yml": "",
            },
        )


def test_err_duplicate_outputs(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
            nodes:
              - node_file: source1.py
              - node_file: source2.py
              - node_file: sink.py""",
            "source1.py": "port=OutputStream",
            "source2.py": "port=OutputStream",
            "sink.py": "port=InputStream",
        },
    )

    assert_nodes(
        manifest,
        n("source1", errors=["Duplicate output 'port'"]),
        n("source2", errors=["Duplicate output 'port'"]),
        n("sink"),
    )

    node = manifest.get_single_node_by_name("source1")
    assert list(manifest.get_errors_for_node(node)) == [
        GraphError(node_id=node.id, message="Duplicate output 'port'")
    ]


def test_err_unresolved_ports(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                nodes:
                  - node_file: sub/graph.yml""",
            "sub/graph.yml": """
                exposes:
                  inputs:
                    - subi
                  outputs:
                    - subo
                nodes:
                  - node_file: node.py""",
            "sub/node.py": "subi=InputStream, subo=OutputStream",
        },
    )

    assert_nodes(
        manifest,
        n("node", parent="sub"),
        n("sub", node_type=NodeType.Graph, errors=['Cannot find output named "subi"']),
    )


def test_err_exposing_nonexistant_ports(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                nodes:
                  - node_file: sub/graph.yml""",
            "sub/graph.yml": """
                exposes:
                  inputs:
                    - noin
                  outputs:
                    - noout""",
        },
    )

    assert_nodes(
        manifest,
        n(
            "sub",
            node_type=NodeType.Graph,
            errors=[
                'Exposed input does not exist: "noin"',
                'Exposed output does not exist: "noout"',
            ],
        ),
    )


def test_err_invalid_node_file(tmp_path: Path):
    manifest = setup_manifest(
        tmp_path,
        {
            "graph.yml": """
            nodes:
              - node_file: oksource.py
              - node_file: oksql.sql
              - node_file: badpy.py
              - node_file: badsql.sql""",
            "oksource.py": """
            @node
            def foo(table=OutputTable):
                pass
            
            undefined[0] = undefined
            """,
            "oksql.sql": "{{ InputTable('table') }} syntax error",
            "badpy.py": "foo, bar",
            "badsql.sql": "SELECT * FROM {{ Err('table') }}",
        },
    )

    assert_nodes(
        manifest,
        n("oksource", local_edges=["oksource:table -> oksql:table"]),
        n("oksql", local_edges=["oksource:table -> oksql:table"]),
        n(
            "badpy",
            errors=[
                "Error parsing file badpy.py: Node function generated_node must specify an input, output, or parameter for all arguments."
            ],
        ),
        n("badsql", errors=["Error parsing file badsql.sql: 'Err' is undefined"]),
    )


def test_err_invalid_webhook_entry(tmp_path: Path):
    with pytest.raises(ValidationError):
        setup_manifest(
            tmp_path,
            {
                "graph.yml": """
                    nodes:
                      - node_file: node.py
                        webhook: err
                """,
            },
        )


def test_err_invalid_chart_entry(tmp_path: Path):
    with pytest.raises(ValidationError):
        setup_manifest(
            tmp_path,
            {
                "graph.yml": """
                    nodes:
                      - node_file: chart.json
                        chart_input: input
                        schedule: error
                """,
            },
        )
