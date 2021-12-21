from pathlib import Path

from basis.cli.config import read_local_basis_config
from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_login(tmp_path: Path):
    cfg_pth = set_tmp_dir(tmp_path, create_basis_config=False)
    with request_mocker() as m:
        m.get(
            API_BASE_URL + Endpoints.ORGANIZATIONS_LIST,
            json={"results": [{"uid": "org-1-uid", "name": "org-1"}], "next": None},
        )
        m.get(
            API_BASE_URL + Endpoints.environments_list("org-1-uid"),
            json={"results": [{"uid": "env-1-uid", "name": "env-1"}], "next": None},
        )

        result = run_cli("login --email=a@e.com --password=pass")

    assert cfg_pth.is_file()
    config = read_local_basis_config()
    assert config.token == "access-token"
    assert "org-1" in result.output
