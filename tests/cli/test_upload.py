from pathlib import Path

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_upload(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = "/".join((dr / "name").parts)
    run_cli(f"create graph {path}")

    with request_mocker() as m:
        for e in [
            Endpoints.graph_version_create("test-org-uid"),
            Endpoints.DEPLOYMENTS_DEPLOY,
        ]:
            m.post(
                API_BASE_URL + e,
                json={"uid": "1", "ui_url": "url.com", "graph": {"name": "g"}},
            )
        result = run_cli(f"upload {path}")
        assert "Uploaded new graph" in result.output
        assert "Graph deployed" in result.output
        assert "url.com" in result.output

        result = run_cli(f"upload --no-deploy {path}")
        assert "Uploaded new graph" in result.output
        assert "Graph deployed" not in result.output


def test_upload_component(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = "/".join((dr / "name").parts)
    run_cli(f"create graph {path}")
    run_cli(f"create node {path}/node.py")

    with request_mocker() as m:
        for e in [
            Endpoints.graph_version_create("test-org-uid"),
            Endpoints.DEPLOYMENTS_DEPLOY,
        ]:
            m.post(
                API_BASE_URL + e,
                json={"uid": "1", "ui_url": "url.com", "graph": {"name": "g"}},
            )
        m.post(
            API_BASE_URL + Endpoints.COMPONENTS_CREATE, json={"uid": "1", "name": "c"}
        )
        result = run_cli(
            f"upload --component-path={path}/node.py --component-name=c {path}"
        )
        assert "Uploaded new graph" in result.output
        assert "Graph deployed" in result.output
        assert "Published graph component" in result.output
