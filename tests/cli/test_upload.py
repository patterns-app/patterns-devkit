from pathlib import Path

from patterns.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_upload(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    path.mkdir()
    graph_file = path / "graph.yml"
    text_before = """
name: name
slug: test-graph
exposes:
  outputs:
    - output
functions:
  - node_file: p.py
""".lstrip()
    graph_file.write_text(text_before)
    (path / "p.py").write_text(
        """
from patterns import *
@node
def node_fn(output=OutputTable):
    pass
"""
    )

    with request_mocker() as m:
        m.post(
            API_BASE_URL + Endpoints.graph_version_create("test-org-uid"),
            json={
                "uid": "1",
                "ui_url": "url.com",
                "graph": {"name": "g"},
                "errors": [{"node_id": "n1", "message": "Test Error"}],
            },
        )
        result = run_cli(f"upload {path}")
        assert "Uploaded new graph" in result.output
        assert "Test Error" in result.output
        assert "url.com" in result.output

    text_after = graph_file.read_text()
    assert text_after[: len(text_before)] == text_before
    assert "id: " in text_after


def test_upload_component(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = "/".join((dr / "name").parts)
    run_cli(f"create graph {path}")
    run_cli(f"create node {path}/node.py")

    with request_mocker() as m:
        m.post(
            API_BASE_URL + Endpoints.graph_version_create("test-org-uid"),
            json={
                "uid": "1",
                "ui_url": "url.com",
                "graph": {"name": "g"},
                "errors": [],
            },
        )
        m.post(
            API_BASE_URL + Endpoints.COMPONENTS_CREATE,
            json={
                "uid": "2",
                "version_name": "1.1.1",
                "component": {"uid": "3", "slug": "c"},
                "organization": {"uid": "4", "slug": "o"},
            },
        )
        result = run_cli(f"upload --publish-component {path}")
        assert "Uploaded new graph" in result.output
        assert "Published graph component" in result.output


def test_upload_custom_yaml_name(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    path.mkdir()
    graph_file = path / "custom.yml"
    graph_file.write_text(
        """
name: name
stores:
 - table: t
""".lstrip()
    )

    with request_mocker() as m:
        m.post(
            API_BASE_URL + Endpoints.graph_version_create("test-org-uid"),
            json={
                "uid": "1",
                "ui_url": "url.com",
                "graph": {"name": "g"},
                "manifest": {},
            },
        )
        result = run_cli(f"upload {graph_file.as_posix()}")
        assert "Uploaded new graph" in result.output
