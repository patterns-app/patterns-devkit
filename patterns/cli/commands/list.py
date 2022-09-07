import json
from typing import Iterable

import typer
from rich.table import Table
from typer import Option

from patterns.cli.services.list import paginated_graphs
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.organizations import paginated_organizations
from patterns.cli.services.output import sprint, abort_on_error

_type_help = "The type of object to list"
_json_help = "Output the object as JSON Lines"
_organization_help = "The name of the Patterns organization to use"

_organization_option = Option("", "--organization", "-o", help=_organization_help)

list_command = typer.Typer(name="list", help="List objects of a given type")


@list_command.command()
def graphs(
    organization: str = Option("", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List graphs"""
    ids = IdLookup(organization_name=organization)
    with abort_on_error("Error listing graphs"):
        gs = list(paginated_graphs(ids.organization_id))
    _print_objects(gs, print_json)


@list_command.command()
def organizations(
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List organizations"""
    with abort_on_error("Error listing organizations"):
        es = list(paginated_organizations())
    _print_objects(es, print_json)


def _print_objects(objects: list, print_json: bool, headers: Iterable[str] = ()):
    if not objects:
        if not print_json:
            sprint("[info]No data found")
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
