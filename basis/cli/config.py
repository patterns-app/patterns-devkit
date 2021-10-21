import json
import os
from pathlib import Path
from typing import Dict, Union

BASIS_CONFIG_ENV_VAR = "BASIS_CONFIG"
BASIS_CONFIG_NAME = ".basis-config.json"
DEFAULT_LOCAL_BASIS_CONFIG_PATH = Path("~").expanduser() / BASIS_CONFIG_NAME


def get_basis_config_path() -> str:
    return os.environ.get(BASIS_CONFIG_ENV_VAR, DEFAULT_LOCAL_BASIS_CONFIG_PATH)


def get_default_config() -> Dict:
    return {}


def read_local_basis_config() -> Dict:
    if os.path.exists(get_basis_config_path()):
        with open(get_basis_config_path()) as f:
            return json.load(f)
    else:
        return get_default_config()


def write_local_basis_config(config: Union[str, Dict]):
    with open(get_basis_config_path(), "w") as f:
        if isinstance(config, str):
            f.write(config)
        else:
            json.dump(config, f)


def update_local_basis_config(**values):
    cfg = read_local_basis_config()
    cfg.update(values)
    write_local_basis_config(cfg)


def remove_auth_from_basis_config():
    update_local_basis_config(token="")


def get_current_organization_uid() -> str:
    cfg = read_local_basis_config()
    return cfg["organization_uid"]


def set_current_organization_uid(org_uid: str):
    update_local_basis_config(organization_uid=org_uid)
