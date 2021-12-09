from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import requests_mock

from basis.cli.services.api import API_BASE_URL, Endpoints
from basis.cli.config import BASIS_CONFIG_ENV_VAR, update_local_basis_config


def set_tmp_dir(tmp_dir: Path, create_basis_config: bool = True) -> Path:
    cfg_pth = Path(tmp_dir) / ".basis-config.json"
    os.environ[BASIS_CONFIG_ENV_VAR] = str(cfg_pth)
    if create_basis_config:
        update_local_basis_config(token="test-token", organization_name="test-org-uid")
    return cfg_pth


@contextmanager
def request_mocker():
    with requests_mock.Mocker() as m:
        m.post(
            API_BASE_URL + Endpoints.TOKEN_CREATE,
            json={"access": "access-token", "refresh": "refresh-token"},
        )
        m.post(API_BASE_URL + Endpoints.TOKEN_VERIFY)
        yield m
