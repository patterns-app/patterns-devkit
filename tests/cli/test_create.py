from pathlib import Path

from tests.cli.base import set_tmp_dir, run_cli


def test_generate_graph(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    name = "testgraph"
    run_cli("create graph", f"{dr / name}\n")
    assert name in (Path(dr) / name / "graph.yml").read_text()


def test_generate_graph_explicit(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    name = "testgraph"
    path = dr / "pth" / "projname"
    run_cli(f"create graph --name={name} '{path}'")
    assert name in (Path(dr) / path / "graph.yml").read_text()


def test_generate_node(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "mynode.py"
    run_cli("create graph", f"{dr}\n")
    path = dr / name
    run_cli(f"create node --graph='{dr}'", f"{path}\n")
    assert name in (Path(dr) / "graph.yml").read_text()


def test_generate_node_explicit(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "mynode.py"
    run_cli("create graph", f"{dr}\n")
    path = dr / name
    run_cli(f"create node --graph='{dr}' '{path}'")
    assert name in (Path(dr) / "graph.yml").read_text()
