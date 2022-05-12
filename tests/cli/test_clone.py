import io
from pathlib import Path
from zipfile import ZipFile

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_clone(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    content = "nodes: []"

    with request_mocker() as m:
        b = io.BytesIO()
        with ZipFile(b, "w") as zf:
            zf.writestr("graph.yml", content)
        m.get(
            API_BASE_URL + Endpoints.graph_version_download("uid"),
            content=b.getvalue(),
        )

        result = run_cli(f"clone --version=uid {path}")
        assert "Cloned graph" in result.output
        assert (path / "graph.yml").read_text() == content


def test_pull(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    content = "nodes: []"
    path.mkdir()
    (path / "graph.yml").write_text("")

    with request_mocker() as m:
        b = io.BytesIO()
        with ZipFile(b, "w") as zf:
            zf.writestr("graph.yml", content)
        m.get(
            API_BASE_URL + Endpoints.graph_version_download("uid"),
            content=b.getvalue(),
        )

        result = run_cli(f"pull --graph-version-id=uid -f {path}")
        assert "Pulled graph" in result.output
        assert (path / "graph.yml").read_text() == content
