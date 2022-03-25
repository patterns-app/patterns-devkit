from rich.table import Table
from typer import Option

from basis.cli.config import (
    read_local_basis_config,
    write_local_basis_config,
    get_basis_config_path,
)
from basis.cli.services.environments import get_environment_by_id
from basis.cli.services.lookup import IdLookup
from basis.cli.services.organizations import get_organization_by_id
from basis.cli.services.output import sprint

_config_help = "The name of the organization to use by default"
_environment_help = "The name of the organization to use by default"


def config(
    organization: str = Option("", "-o", "--organization", help=_config_help),
    environment: str = Option("", "-e", "--environment", help=_environment_help),
):
    """Change the default values used by other commands"""
    ids = IdLookup(
        organization_name=organization,
        environment_name=environment,
        ignore_cfg_environment=True,
    )
    if organization:
        ids.cfg.organization_id = ids.organization_id
    if environment:
        ids.cfg.environment_id = ids.environment_id
    write_local_basis_config(ids.cfg)

    sprint(f"[info]Your basis config is located at "
           f"[code]{get_basis_config_path().as_posix()}")

    t = Table(show_header=False)
    try:
        name = get_organization_by_id(ids.organization_id)["name"]
        t.add_row("organization", name)
    except Exception:
        t.add_row("organization_id", ids.organization_id)
    try:
        name = get_environment_by_id(ids.environment_id)["name"]
        t.add_row("environment", name)
    except Exception:
        t.add_row("environment_id", ids.environment_id)
    if ids.cfg.auth_server:
        t.add_row("auth_server.domain", ids.cfg.auth_server.domain)
        t.add_row("auth_server.audience", ids.cfg.auth_server.audience)
        t.add_row("auth_server.devkit_client_id", ids.cfg.auth_server.devkit_client_id)
    sprint(t)
