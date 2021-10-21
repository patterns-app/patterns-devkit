import os
from pathlib import Path

import requests_mock
from basis.cli.services.api import API_BASE_URL, Endpoints
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
from tests.cli.base import IS_CI, get_test_command, reqest_mocker, set_tmp_dir


def test_login():
    dr = set_tmp_dir()
    cfg_pth = Path(dr) / ".basis-config.json"
    os.environ[BASIS_CONFIG_ENV_VAR] = str(cfg_pth)
    command_tester = get_test_command("login")
    un = "username"
    pw = "password"
    with reqest_mocker() as m:
        m.get(
            API_BASE_URL + Endpoints.ORGANIZATIONS_LIST,
            json=[{"uid": "org-1-uid", "name": "org-1"}],
        )
        inputs = "\n".join([un]) + "\n"
        command_tester.execute(f"-p {pw}", inputs=inputs)

        assert os.path.exists(cfg_pth)
        config = read_local_basis_config()
        assert config.get("token") == "access-token"
