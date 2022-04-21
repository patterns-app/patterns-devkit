from pathlib import Path

from basis.cli.services.api import Endpoints, API_BASE_URL
from tests.cli.base import set_tmp_dir, run_cli, request_mocker


def test_trigger_node_in_subgraph(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent / "graph"
    name = "sub/graph.yml"
    run_cli("create graph", f"{dr}\n")
    path = dr / name
    run_cli(f"create node", f"{path}\n")
    name = (dr / "sub/p.py").as_posix()
    run_cli(f"create node", f"{name}\n")

    with request_mocker() as m:
        m.post(
            API_BASE_URL + Endpoints.DEPLOYMENTS_TRIGGER_NODE, json={"uid": "1"},
        )
        m.get(
            API_BASE_URL + Endpoints.graph_by_slug("test-org-uid", "graph"),
            json={"uid": "2"},
        )
        m.get(
            API_BASE_URL + Endpoints.graphs_latest("2"),
            json={"active_graph_version": {"uid": "3"}},
        )
        result = run_cli(f"trigger {name}")
        assert "Triggered node" in result.output
