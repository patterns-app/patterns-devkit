import re
import textwrap
from pathlib import Path

from basis.configuration.edit import GraphConfigEditor


def test_round_trip(tmp_path: Path):
    s = """
    title: graph
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
        id: <id>
    """
    get_editor(tmp_path, before).add_node("node.py").assert_dump(after)


def test_add_node_to_empty_graph(tmp_path: Path):
    before = """
    title: graph
    """
    after = """
    title: graph
    nodes:
      - node_file: node.py
        id: <id>
    """
    get_editor(tmp_path, before).add_node("node.py").assert_dump(after)


def test_add_webhook_with_all_fields(tmp_path: Path):
    before = """
    title: graph
    """
    after = """
    title: graph
    nodes:
      - webhook: hook
        title: n
        id: ab234567
        description: desc
    """
    get_editor(tmp_path, before).add_webhook(
        "hook", "n", "ab234567", "desc"
    ).assert_dump(after)


def test_add_node_with_all_fields(tmp_path: Path):
    before = """
    title: graph
    nodes:
      - webhook: hook
    """
    after = """
    title: graph
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
        title: my node
        id: ab234567
        description: desc
    """
    get_editor(tmp_path, before).add_node(
        "node.py",
        schedule="daily",
        inputs=["hook -> node_in"],
        outputs=["node_out -> my_table"],
        parameters={"limit": 2},
        title="my node",
        id="ab234567",
        description="desc",
    ).assert_dump(after)


def test_add_component_with_all_fields(tmp_path: Path):
    before = """
      title: graph
      nodes:
        - webhook: hook
      """
    after = """
      title: graph
      nodes:
        - webhook: hook
        - uses: org/component@v1
          schedule: daily
          inputs:
            - hook -> node_in
          outputs:
            - node_out -> my_table
          parameters:
            limit: 2
          title: my node
          id: ab234567
          description: desc
      """
    get_editor(tmp_path, before).add_component_uses(
        "org/component@v1",
        schedule="daily",
        inputs=["hook -> node_in"],
        outputs=["node_out -> my_table"],
        parameters={"limit": 2},
        title="my node",
        id="ab234567",
        description="desc",
    ).assert_dump(after)


def test_remove_nodes(tmp_path: Path):
    before = """
    nodes:
      - node_file: a.py
        title: a1
      - node_file: b.py
        title: b
      - node_file: a.py
        title: a2
    """
    after = """
    nodes:
      - node_file: a.py
        title: a1
      - node_file: b.py
        title: b
    """
    get_editor(tmp_path, before).remove_node_with_id(
        "a2", lambda n: n["title"]
    ).assert_dump(after)


def test_add_missing_node_ids(tmp_path: Path):
    before = """
    nodes:
      - node_file: a.py
        title: a
      - node_file: b.py
        id: foo
      - node_file: c.py
    """
    after = """
    nodes:
      - node_file: a.py
        title: a
        id: <id>
      - node_file: b.py
        id: <id>
      - node_file: c.py
        id: <id>
    """
    editor = get_editor(tmp_path, before).add_missing_node_ids()
    dump = editor.assert_dump(after)
    assert "id: foo" in dump


def get_editor(tmp_path: Path, s: str) -> "_EditorTester":
    f = tmp_path / "graph.yml"
    s = textwrap.dedent(s).strip()
    f.write_text(s)
    return _EditorTester(f)


class _EditorTester(GraphConfigEditor):
    def assert_dump(self, s: str) -> str:
        s = textwrap.dedent(s).strip()
        dump = self.dump().strip()
        if "<id>" in s:
            dump = re.sub(r"id: \w+", "id: <id>", dump)
        assert dump == s
        return self.dump().strip()
