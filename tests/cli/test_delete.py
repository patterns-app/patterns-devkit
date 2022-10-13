from pathlib import Path

from patterns.cli.services.api import Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_delete(tmp_path: Path):
    set_tmp_dir(tmp_path)

    with request_mocker() as m:
        m.delete(Endpoints.graph_delete("2"))
        m.get(Endpoints.graph_by_slug("test-org-uid", "test-graph"), json={"uid": "2"})

        result = run_cli(f"delete -f test-graph")
        assert "App deleted" in result.output
