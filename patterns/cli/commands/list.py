import json
from typing import Iterable

import typer
from rich.table import Table
from typer import Option

from patterns.cli.services.graph_list import paginated_graphs
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.organizations import paginated_organizations
from patterns.cli.services.output import sprint, abort_on_error
from patterns.cli.services.secrets import paginated_secrets

_type_help = "The type of object to list"
_json_help = "Output the object as JSON Lines"
_organization_help = "The name of the Patterns organization to use"

_organization_option = Option("", "--organization", "-o", help=_organization_help)

list_command = typer.Typer(name="list", help="List objects of a given type")


@list_command.command()
def apps(
    organization: str = Option("", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List all apps in your organization"""
    ids = IdLookup(organization_name=organization)
    with abort_on_error("Error listing apps"):
        gs = list(paginated_graphs(ids.organization_uid))
    _print_objects("apps", gs, print_json)


# deprecated alias for `list apps`
@list_command.command(hidden=True)
def graphs(
    organization: str = Option("", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    apps(organization, print_json)


@list_command.command()
def organizations(
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List all organizations you are a member of"""
    with abort_on_error("Error listing organizations"):
        es = list(paginated_organizations())
    _print_objects("organizations", es, print_json)


@list_command.command()
def secrets(
    organization: str = Option("", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List all secrets in your organization"""
    ids = IdLookup(organization_name=organization)
    with abort_on_error("Error listing secrets"):
        ss = list(paginated_secrets(ids.organization_uid))
    _print_objects("secrets", ss, print_json)


def _print_objects(name: str, objects: list, print_json: bool, headers: Iterable[str] = ()):
    if not objects:
        if not print_json:
            sprint(f"[info]No {name} found")
        return

    if print_json:
        for o in objects:
            print(json.dumps(o))
    else:
        table = Table()
        for k in headers:
            table.add_column(k)
        for k in objects[0].keys():
            if k not in headers:
                table.add_column(k)
        columns = [str(c.header) for c in table.columns]
        for o in objects:
            table.add_row(*(str(o.get(c, "")) for c in columns))
        sprint(table)
