import os
from pathlib import Path
from typing import Optional

import platformdirs
import pydantic

DEVKIT_CONFIG_ENV_VAR = "PATTERNS_CONFIG"
DEVKIT_CONFIG_NAME = "config.json"


class AuthServer(pydantic.BaseModel):
    domain: str
    audience: str
    devkit_client_id: str


class CliConfig(pydantic.BaseModel):
    organization_id: str = None
    environment_id: str = None
    token: str = None
    refresh: str = None
    auth_server: AuthServer = None

    class Config:
        extra = "ignore"
        allow_population_by_field_name = True


def get_devkit_config_path() -> Path:
    path = os.environ.get(DEVKIT_CONFIG_ENV_VAR)
    if path:
        return Path(path)
    config_dir = platformdirs.user_config_dir("patterns", appauthor=False, roaming=True)
    return Path(config_dir) / DEVKIT_CONFIG_NAME


def read_devkit_config() -> CliConfig:
    path = get_devkit_config_path()
    if path.exists():
        return CliConfig.parse_file(path)
    return CliConfig()


def write_devkit_config(config: CliConfig):
    path = get_devkit_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(config.json(indent="  "))


_UNCHANGED = object()


def update_devkit_config(
    organization_id: Optional[str] = _UNCHANGED,
    environment_id: Optional[str] = _UNCHANGED,
    token: Optional[str] = _UNCHANGED,
    refresh: Optional[str] = _UNCHANGED,
    auth_server: Optional[AuthServer] = _UNCHANGED,
) -> CliConfig:
    cfg = read_devkit_config()
    update = {}
    if organization_id != _UNCHANGED:
        update["organization_id"] = organization_id
    if environment_id != _UNCHANGED:
        update["environment_id"] = environment_id
    if token != _UNCHANGED:
        update["token"] = token
    if refresh != _UNCHANGED:
        update["refresh"] = refresh
    if auth_server != _UNCHANGED:
        update["auth_server"] = auth_server
    copy = cfg.copy(update=update)
    write_devkit_config(copy)
    return copy
