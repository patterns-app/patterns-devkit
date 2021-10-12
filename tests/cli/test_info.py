from basis.cli.api import DEFAULT_BASE_URL
from basis.cli.config import BASIS_CONFIG_ENV_VAR
import os
from pathlib import Path
from typing import Tuple
import requests_mock

from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_info():
    if not IS_CI:
        dr = set_tmp_dir()
        cfg_pth = Path(dr) / ".basis-config.json"
        os.environ[BASIS_CONFIG_ENV_VAR] = str(cfg_pth)
        command_tester = get_test_command("info")
        with requests_mock.Mocker() as m:
            m.post(
                DEFAULT_BASE_URL + "project/info", json={"name": "name"},
            )
            m.post(
                DEFAULT_BASE_URL + "app/info", json={"name": "name"},
            )
            m.post(
                DEFAULT_BASE_URL + "node/info", json={"name": "name"},
            )
            command_tester.execute(f"project name")
            assert "name" in command_tester.io.fetch_output()
            command_tester.execute(f"app name")
            assert "name" in command_tester.io.fetch_output()
            command_tester.execute(f"node name")
            assert "name" in command_tester.io.fetch_output()

