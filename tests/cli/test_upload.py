from basis.cli.commands.upload import DEFAULT_PROJECT_UPLOAD_ENDPOINT
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
import os
import random
import tempfile
from pathlib import Path
from typing import Tuple
from basis.cli.commands.auth import DEFAULT_LOGIN_ENDPOINT
import requests_mock

import pytest
from basis.cli.app import app
from basis.cli.commands.generate import GenerateCommand
from cleo import Application, CommandTester
from cleo.testers import command_tester
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
            m.post(DEFAULT_PROJECT_UPLOAD_ENDPOINT, json={"project_version_id": 1})
            command_tester.execute(f"{proj_path / 'basis.yml'}")

