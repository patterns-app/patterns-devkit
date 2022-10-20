import json

from rich.table import Table
from typer import Option

from patterns.cli.config import (
    write_devkit_config,
    get_devkit_config_path,
)
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint

_config_help = "Set the name of the organization to use by default"
_json_help = "Output the config as JSON"


def config(
    organization: str = Option("", "-o", "--organization", help=_config_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """Get or set the default values used by other commands"""
    ids = IdLookup(
        organization_slug=organization,
    )
    if organization:
        ids.cfg.organization_id = ids.organization_uid
        write_devkit_config(ids.cfg)
    config_path = get_devkit_config_path().as_posix()

    rows = {}
    try:
        rows["organization"] = ids.organization_name
    except Exception:
        rows["organization_id"] = ids.organization_uid
    if ids.cfg.auth_server:
        rows["auth_server.domain"] = ids.cfg.auth_server.domain
        rows["auth_server.audience"] = ids.cfg.auth_server.audience
        rows["auth_server.devkit_client_id"] = ids.cfg.auth_server.devkit_client_id
    if print_json:
        rows["config file"] = config_path
        print(json.dumps(rows))
    else:
        sprint(f"[info]Your patterns config is located at " f"[code]{config_path}")
        t = Table(show_header=False)
        for k, v in rows.items():
            t.add_row(k, v)
        sprint(t)
