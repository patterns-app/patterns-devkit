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

    sprint(f"[info]Your basis config is located [code]{get_basis_config_path()}")

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
    t.add_row("email", ids.cfg.email)
    for k, v in ids.cfg.dict(exclude_none=True).items():
        if k in ("organization_id", "environment_id", "email"):
            continue
        t.add_row(k, v)
    sprint(t)
