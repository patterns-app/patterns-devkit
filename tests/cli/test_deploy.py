from pathlib import Path

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_deploy(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"

    with request_mocker() as m:
        for e in [
            Endpoints.graph_version_create("test-org-uid"),
            Endpoints.DEPLOYMENTS_DEPLOY,
        ]:
            m.post(
                API_BASE_URL + e,
                json={"uid": "1", "ui_url": "url.com", "graph": {"name": "g"}, "manifest":{}},
            )
        for e in [
            Endpoints.graph_by_slug("test-org-uid", "name"),
            Endpoints.graphs_latest("1"),
        ]:
            m.get(
                API_BASE_URL + e,
                json={"uid": "1", "active_graph_version": {"uid": "1"}},
            )

        run_cli(f"create graph {path}")

        result = run_cli(f"upload --no-deploy {path}")
        assert "Uploaded new graph" in result.output
        assert "Graph deployed" not in result.output

        result = run_cli(f"deploy --graph={path}")
        assert "deployed" in result.output
