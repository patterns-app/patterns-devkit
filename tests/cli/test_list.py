from pathlib import Path

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_list(tmp_path: Path):
    set_tmp_dir(tmp_path)
    with request_mocker() as m:
        for e in [
            Endpoints.ENVIRONMENTS_LIST,
            Endpoints.GRAPHS_LIST,
        ]:
            m.get(
                API_BASE_URL + e, json={"results": [{"name": "name"}]},
            )
        result = run_cli("list environments --json")
        assert "name" in result.output
        result = run_cli("list graphs --json")
        assert "name" in result.output
