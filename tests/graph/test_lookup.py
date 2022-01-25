from pathlib import Path

from basis.cli.services.lookup import IdLookup
from tests.graph.utils import setup_manifest


def test_find_graph_from_node(tmp_path: Path):
    setup_manifest(
        tmp_path,
        {
            "graph.yml": """
                nodes:
                  - node_file: node1.py
                  - node_file: dir/node2.py
                  - node_file: sub/graph.yml
            """,
            "node1.py": "t3=OutputTable",
            "dir/node2.py": "t1=OutputTable",
            "sub/graph.yml": """
                nodes:
                  - node_file: node3.py
            """,
            "sub/node3.py": "t2=OutputTable",
        },
    )
    for p in [
        tmp_path / "dir" / "node1.py",
        tmp_path / "sub" / "node2.py",
        tmp_path / "node3.py",
    ]:
        actual = IdLookup(node_file_path=p).graph_file_path
        assert actual == tmp_path / "graph.yml"
