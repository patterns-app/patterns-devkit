from pathlib import Path

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_upload(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    path.mkdir()
    graph_file = path / "graph.yml"
    text_before = """
name: name
exposes:
  outputs:
    - output
functions:
  - node_file: p.py
""".lstrip()
    graph_file.write_text(text_before)
    (path / "p.py").write_text(
        """
from basis import *
@node
def node_fn(output=OutputTable):
    pass
"""
    )

    with request_mocker() as m:
        for e in [
            Endpoints.graph_version_create("test-org-uid"),
            Endpoints.DEPLOYMENTS_DEPLOY,
        ]:
            m.post(
                API_BASE_URL + e,
                json={
                    "uid": "1",
                    "ui_url": "url.com",
                    "graph": {"name": "g"},
                    "manifest": {
                        "errors": [{"node_id": "n1", "message": "Test Error"}]
                    },
                },
            )
        result = run_cli(f"upload {path}")
        assert "Uploaded new graph" in result.output
        assert "Test Error" in result.output
        assert "Graph deployed" in result.output
        assert "url.com" in result.output

        result = run_cli(f"upload --no-deploy {path}")
        assert "Uploaded new graph" in result.output
        assert "Graph deployed" not in result.output

    text_after = graph_file.read_text()
    assert text_after[: len(text_before)] == text_before
    assert "id: " in text_after


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
                json={
                    "uid": "1",
                    "ui_url": "url.com",
                    "graph": {"name": "g"},
                    "manifest": {"errors": []},
                },
            )
        m.post(
            API_BASE_URL + Endpoints.COMPONENTS_CREATE,
            json={
                "uid": "2",
                "version_names": ["v1", "v1.1"],
                "component": {"uid": "3", "slug": "c"},
                "organization": {"uid": "4", "slug": "o"},
            },
        )
        result = run_cli(f"upload --publish-component {path}")
        assert "Uploaded new graph" in result.output
        assert "Published graph component" in result.output
