from pathlib import Path

from patterns.cli.services.api import Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_list_graphs(tmp_path: Path):
    set_tmp_dir(tmp_path)
    with request_mocker() as m:
        m.get(
            Endpoints.graphs_list("test-org-uid"),
            json={"results": [{"name": "name"}], "next": None},
        )
        result = run_cli("list apps --json")
        assert "name" in result.output
