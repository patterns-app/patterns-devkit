import textwrap
from pathlib import Path

from basis.configuration.edit import GraphConfigEditor
from basis.configuration.graph import ExposingCfg, GraphDefinitionCfg, NodeCfg
from basis.configuration.path import NodeId
from basis.graph.configured_node import (
    GraphManifest,
    CURRENT_MANIFEST_SCHEMA_VERSION,
    ConfiguredNode,
    NodeType,
)
from tests.graph.utils import setup_manifest


def test_round_trip(tmp_path: Path):
    s = """
    name: graph
    nodes:
      - webhook: out # eol comment
      # node 1
      - node_file: node_1.py
        inputs:
          - out -> in
    """
    get_editor(tmp_path, s).assert_dump(s)


def test_round_trip_no_indent(tmp_path: Path):
    s = """
    nodes:
    - webhook: out # eol comment
    - node_file: node_1.py
      inputs:
      - out -> in
    """
    get_editor(tmp_path, s).assert_dump(s)


def test_add_node_to_existing_nodes(tmp_path: Path):
    before = """
    nodes:
      - webhook: out # eol comment
    """
    after = """
    nodes:
      - webhook: out # eol comment
      - node_file: node.py
    """
    get_editor(tmp_path, before).add_node("node.py").assert_dump(after)


def test_add_node_to_empty_graph(tmp_path: Path):
    before = """
    name: graph
    """
    after = """
    name: graph
    nodes:
      - node_file: node.py
    """
    get_editor(tmp_path, before).add_node("node.py").assert_dump(after)


def test_add_webhook_with_all_fields(tmp_path: Path):
    before = """
    name: graph
    """
    after = """
    name: graph
    nodes:
      - webhook: hook
        name: n
        id: ab234567
        description: desc
    """
    get_editor(tmp_path, before).add_webhook(
        "hook", "n", "ab234567", "desc"
    ).assert_dump(after)


def test_add_node_with_all_fields(tmp_path: Path):
    before = """
    name: graph
    nodes:
      - webhook: hook
    """
    after = """
    name: graph
    nodes:
      - webhook: hook
      - node_file: node.py
        schedule: daily
        inputs:
          - hook -> node_in
        outputs:
          - node_out -> my_table
        parameters:
          limit: 2
        name: my node
        id: ab234567
        description: desc
    """
    get_editor(tmp_path, before).add_node(
        "node.py",
        schedule="daily",
        inputs=["hook -> node_in"],
        outputs=["node_out -> my_table"],
        parameters={"limit": 2},
        name="my node",
        id="ab234567",
        description="desc",
    ).assert_dump(after)


def test_parsing():
    b = GraphConfigEditor(None, read=False)
    b.set_name("test")
    b.add_webhook("hook")
    manifest = b.parse_to_manifest()

    assert manifest.graph_name == "test"
    assert manifest.errors == []
    assert len(manifest.nodes) == 1
    assert manifest.nodes[0].name == "hook"
    assert manifest.nodes[0].node_type == NodeType.Webhook

    exposes = ExposingCfg(outputs=["hook"])
    b.set_exposing_cfg(exposes)
    cfg = b.parse_to_cfg()
    assert cfg == GraphDefinitionCfg(
        name="test", exposes=exposes, nodes=[NodeCfg(webhook="hook")]
    )


def test_parsing_with_subgraph(tmp_path: Path):
    setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                name: foo
                nodes:
                  - node_file: sub/graph.yml
            """,
            "sub/graph.yml": """
                exposes:
                  outputs:
                    - sub_out
                nodes:
                  - webhook: sub_out
            """,
        },
    )
    path = tmp_path / "graph.yml"
    before = path.read_text()
    b = GraphConfigEditor(path)
    b.set_name("bar")
    m = b.parse_to_manifest()
    assert m.graph_name == "bar"
    assert len(m.nodes) == 2
    assert path.read_text() == before


def get_editor(tmp_path: Path, s: str) -> "_EditorTester":
    f = tmp_path / "graph.yml"
    s = textwrap.dedent(s).strip()
    f.write_text(s)
    return _EditorTester(f)


class _EditorTester(GraphConfigEditor):
    def assert_dump(self, s: str):
        s = textwrap.dedent(s).strip()
        assert self.dump().strip() == s
