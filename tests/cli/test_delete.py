from pathlib import Path

from patterns.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_delete(tmp_path: Path):
    set_tmp_dir(tmp_path)

    with request_mocker() as m:
        m.delete(API_BASE_URL + Endpoints.graph_delete("uid"))

        result = run_cli(f"delete -f --graph-id uid")
        assert "Graph deleted" in result.output
