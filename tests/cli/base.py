from __future__ import annotations

import os
import shlex
from contextlib import contextmanager
from pathlib import Path

import click
import requests_mock
import typer.testing
from click.testing import Result

from patterns.cli.config import DEVKIT_CONFIG_ENV_VAR, update_devkit_config
from patterns.cli.main import app
from patterns.cli.services.api import API_BASE_URL, Endpoints, build_url


def run_cli(argv: str, input: str = None, **kwargs) -> click.testing.Result:
    args = ["--stacktrace"] + shlex.split(argv.replace("\\", "/"))
    runner = typer.testing.CliRunner()
    result = runner.invoke(app, args, input, catch_exceptions=False, **kwargs)
    print(result.output)
    return result


def set_tmp_dir(tmp_dir: Path, create_devkit_config: bool = True) -> Path:
    cfg_pth = Path(tmp_dir) / ".test-config.json"
    os.environ[DEVKIT_CONFIG_ENV_VAR] = str(cfg_pth)
    if create_devkit_config:
        update_devkit_config(
            token="test-token",
            organization_id="test-org-uid",
        )
    return cfg_pth


class _BaseUrlMocker(requests_mock.Mocker):
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url

    def request(self, method, url, *args, **kwargs):
        if isinstance(url, str):
            url = build_url(self.base_url, url)
        return super().request(method, url, *args, **kwargs)


@contextmanager
def request_mocker():
    with _BaseUrlMocker(API_BASE_URL) as m:
        m.post(Endpoints.TOKEN_VERIFY)
        yield m
