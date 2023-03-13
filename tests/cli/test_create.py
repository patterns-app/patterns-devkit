from pathlib import Path

from tests.cli.base import set_tmp_dir, run_cli


def test_create_graph(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    name = "testgraph"
    run_cli("create app", f"{dr / name}\n")
    assert name in (dr / name / "graph.yml").read_text()


def test_create_graph_explicit(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    name = "testgraph"
    path = dr / "pth" / "projname"
    run_cli(f"create app --name={name} '{path}'")
    assert name in (dr / path / "graph.yml").read_text()


def test_create_node(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "mynode.py"
    run_cli("create app", f"{dr}\n")
    path = dr / name
    run_cli(f"create node", f"{path}\n")
    assert name in (dr / "graph.yml").read_text()


def test_create_subgraph(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "sub/graph.yml"
    run_cli("create app", f"{dr}\n")
    path = dr / name
    run_cli(f"create node", f"{path}\n")
    assert name in (dr / "graph.yml").read_text()
    assert "sub" in path.read_text()

    name = (dr / "sub/p.py").as_posix()
    run_cli(f"create node", f"{name}\n")
    assert "node_file: p.py" in (dr / "sub/graph.yml").read_text()


def test_create_node_explicit(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "mynode.py"
    run_cli("create app", f"{dr}\n")
    path = dr / name
    run_cli(f"create node '{path}'")
    assert name in (dr / "graph.yml").read_text()
    assert "from patterns import" in path.read_text()


def test_create_node_invalid_py_name(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "0-foo.py"
    run_cli("create app", f"{dr}\n")
    path = dr / name
    run_cli(f"create node", f"{path}\n")
    assert name in (dr / "graph.yml").read_text()
    assert "from patterns import" in path.read_text()


def test_create_webhook(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    run_cli("create app", f"{dr}\n")
    run_cli(f"create node --app={dr} --type=webhook hook")
    text = (dr / "graph.yml").read_text()
    assert "webhook: hook" in text
    assert "table: hook" in text


def test_create_component(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    run_cli("create app", f"{dr}\n")
    run_cli(f"create node --type=component --app={dr} foo/bar@v1")
    assert f"uses: foo/bar@v1" in (dr / "graph.yml").read_text()


def test_create_webhook_deprecated(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    run_cli("create app", f"{dr}\n")
    run_cli(f"create webhook --app={dr} hook")
    assert f"webhook: hook" in (dr / "graph.yml").read_text()


def test_create_component_deprecated(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    run_cli("create app", f"{dr}\n")
    run_cli(f"create node --component=foo/bar@v1 --app={dr}")
    assert f"uses: foo/bar@v1" in (dr / "graph.yml").read_text()


def test_create_table(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    run_cli("create app", f"{dr}\n")
    run_cli(f"create node --app={dr} --type=table tbl")
    text = (dr / "graph.yml").read_text()
    assert "table: tbl" in text
