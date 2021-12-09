from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import requests_mock
from basis.cli.app import app
from basis.cli.services.api import API_BASE_URL, Endpoints
from cleo import CommandTester

from cli.config import BASIS_CONFIG_ENV_VAR, update_local_basis_config

IS_CI = os.environ.get("CI")


def set_tmp_dir(tmp_dir: Path, create_basis_config: bool = True) -> Path:
    cfg_pth = Path(tmp_dir) / ".basis-config.json"
    os.environ[BASIS_CONFIG_ENV_VAR] = str(cfg_pth)
    if create_basis_config:
        update_local_basis_config(token="test-token", organization_name="test-org-uid")
    return cfg_pth


def get_test_command(name: str) -> CommandTester:
    command = app.find(name)
    command_tester = CommandTester(command)
    return command_tester


@contextmanager
def request_mocker():
    with requests_mock.Mocker() as m:
        m.post(
            API_BASE_URL + Endpoints.TOKEN_CREATE,
            json={"access": "access-token", "refresh": "refresh-token"},
        )
        m.post(API_BASE_URL + Endpoints.TOKEN_VERIFY )
        yield m
