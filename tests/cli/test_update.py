import io
from pathlib import Path
from zipfile import ZipFile

from patterns.cli.services.api import Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_update(tmp_path: Path):
    set_tmp_dir(tmp_path)

    with request_mocker() as m:
        m.patch(Endpoints.component_update("uid"))
        m.patch(Endpoints.graph_update("uid"))
        m.get(
            Endpoints.graph_by_slug("test-org-uid", "mygraph"),
            json={"slug": "uid", "uid": "uid"},
        )
        result = run_cli(f"update app mygraph --deprecated")
        assert "Updated app" in result.output
        assert m.last_request.json() == {"deprecated": True}

        result = run_cli(f"update app mygraph --public")
        assert "Updated app" in result.output
        assert m.last_request.json() == {"public": True}

        result = run_cli(f"update app mygraph --private")
        assert "Updated app" in result.output
        assert m.last_request.json() == {"public": False}

        result = run_cli(f"update app mygraph --no-deprecated")
        assert "Updated app" in result.output
        assert m.last_request.json() == {"deprecated": False}
