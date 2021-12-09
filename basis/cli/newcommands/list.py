import json
from enum import Enum

import typer
from rich.table import Table
from typer import Option

from basis.cli.services.list import list_graphs, list_environments
from basis.cli.services.output import sprint


class Listable(str, Enum):
    env = "environments"
    graph = "graphs"


_type_help = "The type of object to list"
_json_help = "Output the object as JSON Lines"

list_command = typer.Typer(name="list", help="List objects of a given type")


@list_command.command()
def graphs(print_json: bool = Option(False, "--json", help=_json_help)):
    """List graphs"""
    _print_objects(list_graphs(), print_json)


@list_command.command()
def environments(print_json: bool = Option(False, "--json", help=_json_help)):
    """List environments"""
    _print_objects(list_environments(), print_json)


def _print_objects(objects: list, print_json: bool):
    if not objects:
        return

    if print_json:
        for o in objects:
            print(json.dumps(o))
    else:
        table = Table()
        for k in objects[0].keys():
            table.add_column(k)
        for o in objects:
            table.add_row(*(str(v) for v in o.values()))
        sprint(table)
