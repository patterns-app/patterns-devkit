import io
from pathlib import Path
from zipfile import ZipFile

from patterns.cli.services.api import Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_download(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = dr / "name"
    content = "nodes: []"

    with request_mocker() as m:
        b = io.BytesIO()
        with ZipFile(b, "w") as zf:
            zf.writestr("graph.yml", content)
        m.get(Endpoints.graph_version_download("uid"), content=b.getvalue())
        m.get(
            Endpoints.graph_by_slug("test-org-uid", "uid"),
            json={"slug": "uid", "uid": "uid"},
        )
        m.get(
            Endpoints.graphs_latest("uid"),
            json={"active_graph_version": {"uid": "uid"}},
        )

        result = run_cli(f"download uid {path}")
        assert "Downloaded app" in result.output
        assert (path / "graph.yml").read_text() == content
