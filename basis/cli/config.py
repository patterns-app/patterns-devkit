import json
import os
from pathlib import Path
from typing import Dict, Union


LOCAL_BASIS_CONFIG_PATH = Path("~/.basis-config.json").expanduser()


def get_default_config() -> Dict:
    return {}


def read_local_basis_config() -> Dict:
    if os.path.exists(LOCAL_BASIS_CONFIG_PATH):
        with open(LOCAL_BASIS_CONFIG_PATH) as f:
            return json.load(f)
    else:
        return get_default_config()


def write_local_basis_config(config: Union[str, Dict]):
    with open(LOCAL_BASIS_CONFIG_PATH, "w") as f:
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
