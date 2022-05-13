from rich.table import Table
from typer import Option

from patterns.cli.config import (
    read_devkit_config,
    write_devkit_config,
    get_devkit_config_path,
)
from patterns.cli.services.environments import get_environment_by_id
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.organizations import get_organization_by_id
from patterns.cli.services.output import sprint

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
    write_devkit_config(ids.cfg)

    sprint(f"[info]Your patterns config is located at "
           f"[code]{get_devkit_config_path().as_posix()}")

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
