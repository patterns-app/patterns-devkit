from basis.cli.api import DEFAULT_BASE_URL
from basis.cli.config import BASIS_CONFIG_ENV_VAR
import os
from pathlib import Path
from typing import Tuple
import requests_mock

from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_upload():
    if not IS_CI:
        dr = set_tmp_dir()
        proj_path = Path(dr) / "proj"
        # Create project
        get_test_command("create").execute(f"project {proj_path}", inputs="\n")
        cfg_pth = Path(dr) / ".basis-config.json"
        os.environ[BASIS_CONFIG_ENV_VAR] = str(cfg_pth)
        command_tester = get_test_command("upload")
        with requests_mock.Mocker() as m:
            m.post(
                DEFAULT_BASE_URL + "project-version/upload",
                json={"project_version_id": 1},
            )
            command_tester.execute(f"{proj_path / 'basis.yml'}")

