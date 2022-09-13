from rich.table import Table
from typer import Option

from patterns.cli.config import (
    write_devkit_config,
    get_devkit_config_path,
)
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint

_config_help = "Set the name of the organization to use by default"


def config(
    organization: str = Option("", "-o", "--organization", help=_config_help),
):
    """Get or set the default values used by other commands"""
    ids = IdLookup(
        organization_name=organization,
    )
    if organization:
        ids.cfg.organization_id = ids.organization_id
        write_devkit_config(ids.cfg)

    sprint(
        f"[info]Your patterns config is located at "
        f"[code]{get_devkit_config_path().as_posix()}"
    )

    t = Table(show_header=False)
    try:
        t.add_row("organization", ids.organization_name)
    except Exception:
        t.add_row("organization_id", ids.organization_id)
    if ids.cfg.auth_server:
        t.add_row("auth_server.domain", ids.cfg.auth_server.domain)
        t.add_row("auth_server.audience", ids.cfg.auth_server.audience)
        t.add_row("auth_server.devkit_client_id", ids.cfg.auth_server.devkit_client_id)
    sprint(t)
