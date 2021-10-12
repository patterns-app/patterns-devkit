from basis.cli.config import read_local_basis_config
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


def test_login():
    if not IS_CI:
        dr = set_tmp_dir()
        cfg_pth = Path(dr) / "basis-config.json"
        command_tester = get_test_command("login")
        un = "username"
        pw = "password"
        token = "tok"
        with requests_mock.Mocker() as m:
            m.post(DEFAULT_LOGIN_ENDPOINT, json={"token": token})
            inputs = "\n".join([un]) + "\n"
            command_tester.execute(f"-c {cfg_pth} -p {pw}", inputs=inputs)

            assert os.path.exists(cfg_pth)
            config = read_local_basis_config(pth=cfg_pth)
            assert config.get("token") == token

