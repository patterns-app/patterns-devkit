from typer import Option

from basis.cli.config import (
    read_local_basis_config,
    write_local_basis_config,
)
from basis.cli.services.lookup import IdLookup

_config_help = "The name of the organization to use by default"
_environment_help = "The name of the organization to use by default"


def config(
    organization: str = Option("", "-o", "--organization", help=_config_help),
    environment: str = Option("", "-e", "--environment", help=_environment_help),
):
    """Change the default values used by other commands"""
    cfg = read_local_basis_config()
    ids = IdLookup(organization_name=organization, environment_name=environment)
    if organization:
        cfg.organization_id = ids.organization_id
    if environment:
        cfg.environment_id = ids.environment_id
    write_local_basis_config(cfg)
