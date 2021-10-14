import os
import tempfile
from pathlib import Path

from basis.cli.app import app
from basis.cli.config import BASIS_CONFIG_ENV_VAR, update_local_basis_config
from cleo import CommandTester

IS_CI = os.environ.get("CI")


def set_tmp_dir(create_basis_config: bool = False):
    dr = tempfile.mkdtemp()
    os.chdir(dr)
    if create_basis_config:
        create_authed_basis_config(dr)
    return dr


def create_authed_basis_config(pth: str) -> str:
    cfg_pth = Path(pth) / ".basis-config.json"
    os.environ[BASIS_CONFIG_ENV_VAR] = str(cfg_pth)
    update_local_basis_config(token="test-token")


def get_test_command(name: str) -> CommandTester:
    command = app.find(name)
    command_tester = CommandTester(command)
    return command_tester
