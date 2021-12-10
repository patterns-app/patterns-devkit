from typer import Option

from basis.cli.config import (
    read_local_basis_config,
    write_local_basis_config,
)

_config_help = "The name of the organization to use by default"
_environment_help = "The name of the organization to use by default"


def config(
    organization: str = Option("", "--organization", "-o", help=_config_help),
    environment: str = Option("", "--environment", "-e", help=_environment_help),
):
    """Change the default values used by other commands"""
    cfg = read_local_basis_config()
    if organization:
        cfg.organization_name = organization
    if environment:
        cfg.environment_name = environment
    write_local_basis_config(cfg)
