import re
from pathlib import Path

from patterns.cli.services.api import Endpoints
from tests.cli.base import set_tmp_dir, run_cli, request_mocker


def test_trigger_node_in_subgraph(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "sub/graph.yml"
    run_cli("create app", f"{dr}\n")
    path = dr / name
    run_cli(f"create node", f"{path}\n")
    name = (dr / "sub/p.py").as_posix()
    run_cli(f"create node", f"{name}\n")

    with request_mocker() as m:
        id = re.search(r"id: (\w+)", path.read_text()).group(1)
        m.post(Endpoints.trigger_node("2", id), json={"uid": "1"})
        m.get(Endpoints.graph_by_slug("test-org-uid", "graph"), json={"uid": "2"})
        m.get(Endpoints.graphs_latest("2"), json={"active_graph_version": {"uid": "3"}})
        result = run_cli(f"trigger {name}")
        assert "Triggered node" in result.output

        result = run_cli(f"trigger --app=graph --node-id={id}")
        assert "Triggered node" in result.output
