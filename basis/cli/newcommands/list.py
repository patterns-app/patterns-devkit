import json
from enum import Enum

from rich.table import Table
from typer import Option, Argument

from basis.cli.newapp import app
from basis.cli.services.list import list_graphs, list_environments
from basis.cli.services.output import sprint


class Listable(str, Enum):
    env = "environments"
    graph = "graphs"


_type_help = "The type of object to list"
_json_help = "Output the object as JSON Lines"


@app.command()
def list(
    print_json: bool = Option(False, "--json", help=_json_help),
    type: Listable = Argument(..., help=_type_help),
):
    """List all objects of a given type"""
    if type == Listable.graph:
        objects = list_graphs()
    else:
        objects = list_environments()

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
