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
    default_graph: Path = None

    class Config:
        extra = "ignore"


def resolve_graph_path(path: Path, exists: bool) -> Path:
    if path.is_dir():
        for ext in (".yml", ".yaml"):
            f = path / f"graph{ext}"
            if f.is_file():
                if exists:
                    return f.absolute()
                abort(f"Graph '{f}' already exists")
        if exists:
            abort(f"Graph '{f}' does not exist")
        return (path / "graph.yml").absolute()
    if path.suffix and path.suffix not in (".yml", ".yaml"):
        abort(f"Graph '{path}' must be a yaml file")
    if path.is_file():
        if not exists:
            abort(f"Graph '{path}' already exists")
        return path.absolute()
    if exists:
        abort(f"Graph '{path}' does not exist")
    if path.suffix:
        return path.absolute()
    path.mkdir(parents=True)
    graph_path = (path / "graph.yml").absolute()
    update_local_basis_config(default_graph=graph_path)
    return graph_path


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
    path.write_text(config.json())


def update_local_basis_config(**values) -> CliConfig:
    cfg = read_local_basis_config()
    copy = cfg.copy(update=values)
    write_local_basis_config(copy)
    return copy
