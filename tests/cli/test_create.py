from pathlib import Path

from tests.cli.base import set_tmp_dir, run_cli


def test_create_graph(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    name = "testgraph"
    run_cli("create graph", f"{dr / name}\n")
    assert name in (dr / name / "graph.yml").read_text()


def test_create_graph_explicit(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    name = "testgraph"
    path = dr / "pth" / "projname"
    run_cli(f"create graph --name={name} '{path}'")
    assert name in (dr / path / "graph.yml").read_text()


def test_create_node(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "mynode.py"
    run_cli("create graph", f"{dr}\n")
    path = dr / name
    run_cli(f"create node --graph='{dr}'", f"{path}\n")
    assert name in (dr / "graph.yml").read_text()


def test_create_node_explicit(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "mynode.py"
    run_cli("create graph", f"{dr}\n")
    path = dr / name
    run_cli(f"create node --graph='{dr}' '{path}'")
    assert name in (dr / "graph.yml").read_text()
    assert 'def mynode' in path.read_text()


def test_create_node_invalid_py_name(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "0-foo.py"
    run_cli("create graph", f"{dr}\n")
    path = dr / name
    run_cli(f"create node --graph='{dr}'", f"{path}\n")
    assert name in (dr / "graph.yml").read_text()
    assert 'def node_0_foo' in path.read_text()


def test_create_webhook(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    run_cli("create graph", f"{dr}\n")
    run_cli(f"create webhook --graph={dr} hook")
    assert f"webhook: hook" in (dr / "graph.yml").read_text()
