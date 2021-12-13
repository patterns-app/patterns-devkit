import os
from pathlib import Path

import platformdirs

from basis.cli.services.output import abort
from basis.configuration.base import PydanticBase

BASIS_CONFIG_ENV_VAR = "BASIS_CONFIG"
BASIS_CONFIG_NAME = "config.json"


class CliConfig(PydanticBase):
    organization_name: str = None
    environment_name: str = None
    token: str = None
    refresh: str = None
    email: str = None

    class Config:
        extra = "ignore"


def get_basis_config_path() -> Path:
    path = os.environ.get(BASIS_CONFIG_ENV_VAR)
    if path:
        return Path(path)
    config_dir = platformdirs.user_config_dir("basis", appauthor=False, roaming=True)
    return Path(config_dir) / BASIS_CONFIG_NAME


def read_local_basis_config() -> CliConfig:
    path = get_basis_config_path()
    if path.exists():
        return CliConfig.parse_file(path)
    return CliConfig()


def write_local_basis_config(config: CliConfig):
    path = get_basis_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(config.json(indent="  "))


def update_local_basis_config(**values) -> CliConfig:
    cfg = read_local_basis_config()
    copy = cfg.copy(update=values)
    write_local_basis_config(copy)
    return copy
