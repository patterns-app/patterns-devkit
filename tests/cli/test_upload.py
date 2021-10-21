from pathlib import Path

import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_upload():
    dr = set_tmp_dir(create_basis_config=True)
    proj_path = Path(dr) / "proj"
    # Create graph
    get_test_command("generate").execute(f"graph {proj_path}", inputs="\n")
    command_tester = get_test_command("upload")
    with requests_mock.Mocker() as m:
        m.post(
            DEFAULT_BASE_URL + "graph-versions/upload", json={"graph_version_id": 1},
        )
        command_tester.execute(f"{proj_path / 'graph.yml'}")
