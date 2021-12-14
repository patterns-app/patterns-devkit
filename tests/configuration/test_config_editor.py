import textwrap
from pathlib import Path

from basis.configuration.edit import GraphConfigEditor


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


def get_editor(tmp_path: Path, s: str) -> "_EditorTester":
    f = tmp_path / "graph.yml"
    s = textwrap.dedent(s).strip()
    f.write_text(s)
    return _EditorTester(f)


class _EditorTester(GraphConfigEditor):
    def assert_dump(self, s: str):
        s = textwrap.dedent(s).strip()
        assert self.dump().strip() == s
