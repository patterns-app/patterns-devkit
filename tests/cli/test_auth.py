import os
from pathlib import Path

import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_login():
    if not IS_CI:
        dr = set_tmp_dir()
        cfg_pth = Path(dr) / ".basis-config.json"
        os.environ[BASIS_CONFIG_ENV_VAR] = str(cfg_pth)
        command_tester = get_test_command("login")
        un = "username"
        pw = "password"
        token = "tok"
        with requests_mock.Mocker() as m:
            m.post(DEFAULT_BASE_URL + "api/token-auth", json={"token": token})
            inputs = "\n".join([un]) + "\n"
            command_tester.execute(f"-p {pw}", inputs=inputs)

            assert os.path.exists(cfg_pth)
            config = read_local_basis_config()
            assert config.get("token") == token
