from pathlib import Path

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_list(tmp_path: Path):
    set_tmp_dir(tmp_path)
    with request_mocker() as m:
        for e in [
            Endpoints.environments_list("test-org-uid"),
            Endpoints.graphs_list("test-org-uid"),
        ]:
            m.get(
                API_BASE_URL + e, json={"results": [{"name": "name"}], "next": None},
            )
        result = run_cli("list environments --json")
        assert "name" in result.output
        result = run_cli("list graphs --json")
        assert "name" in result.output


def test_list_logs(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    node = path / "node.py"
    run_cli(f"create graph {path}")
    run_cli(f"create node {node}")
    with request_mocker() as m:
        m.get(
            API_BASE_URL + Endpoints.graph_by_name("test-org-uid", "name"),
            json={"uid": "1"},
        )
        m.get(
            API_BASE_URL + Endpoints.EXECUTION_EVENTS,
            json={"results": [{"name": "name"}], "next": None},
        )
        result = run_cli(f"list logs {node} --json")
    assert "name" in result.output


def test_list_data(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    node = path / "node.py"
    run_cli(f"create graph {path}")
    run_cli(f"create node {node}")
    with request_mocker() as m:
        m.get(
            API_BASE_URL + Endpoints.graph_by_name("test-org-uid", "name"),
            json={"uid": "1"},
        )
        m.get(
            API_BASE_URL + Endpoints.OUTPUT_DATA,
            json={"results": [{"name": "name"}], "next": None},
        )
        result = run_cli(f"list output {node} port --json")
    assert "name" in result.output


def test_list_webhooks(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    name1 = "undeployed_webhook"
    name2 = "deployed_webhook"
    run_cli(f"create graph {path}")
    run_cli(f"create webhook --graph='{path}' {name1}")
    with request_mocker() as m:
        m.get(
            API_BASE_URL + Endpoints.graph_by_name("test-org-uid", "name"),
            json={"uid": "1"},
        )
        m.get(
            API_BASE_URL + Endpoints.WEBHOOK_KEYS,
            json={"results": [{"name": name2}], "next": None},
        )
        result = run_cli(f"list webhooks --json {path}")
    assert name1 in result.output
    assert name2 in result.output
