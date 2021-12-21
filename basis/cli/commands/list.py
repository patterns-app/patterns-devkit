import json
from enum import Enum
from pathlib import Path

import typer
from rich.table import Table
from typer import Option, Argument

from basis.cli.services.environments import list_environments
from basis.cli.services.list import list_graphs, list_execution_events, list_output_data
from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, abort_on_error, abort


class Listable(str, Enum):
    env = "environments"
    graph = "graphs"


_type_help = "The type of object to list"
_json_help = "Output the object as JSON Lines"
_organization_help = "The name of the Basis organization to use"
_environment_help = "The name of the Basis environment to use"

_organization_option = Option("", "--organization", "-o", help=_organization_help)
_environment_option = Option("", "--environment", "-e", help=_environment_help)

list_command = typer.Typer(name="list", help="List objects of a given type")


@list_command.command()
def graphs(
    organization: str = Option("", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List graphs"""
    ids = IdLookup(organization_name=organization)
    with abort_on_error("Error listing graphs"):
        gs = list_graphs(ids.organization_id)
    _print_objects(gs, print_json)


@list_command.command()
def environments(
    organization: str = Option("", "--organization", "-o", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List environments"""
    ids = IdLookup(organization_name=organization)
    with abort_on_error("Error listing environments"):
        es = list_environments(ids.organization_id)
    _print_objects(es, print_json)


_node_help = "The path to the node to get logs for"


@list_command.command()
def logs(
    print_json: bool = Option(False, "--json", help=_json_help),
    organization: str = Option("", help=_organization_help),
    environment: str = Option("", help=_environment_help),
    node: Path = Argument(..., exists=True, help=_node_help),
):
    """List execution logs for a node"""
    if bool(organization) != bool(environment):
        abort("Must specify both --organization and --environment, or neither")

    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        node_file_path=node,
    )

    with abort_on_error("Could not list logs"):
        events = list_execution_events(ids.environment_id, ids.graph_id, ids.node_id)
    _print_objects(events, print_json)


_port_help = "The name of the output port"


@list_command.command()
def output(
    print_json: bool = Option(False, "--json", help=_json_help),
    organization: str = Option("", help=_organization_help),
    environment: str = Option("", help=_environment_help),
    node: Path = Argument(..., exists=True, help=_node_help),
    port: str = Argument(..., help=_port_help),
):
    """List data sent to an output port of a from the most recent run of a node"""
    if bool(organization) != bool(environment):
        abort("Must specify both --organization and --environment, or neither")

    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        node_file_path=node,
    )

    with abort_on_error("Could not get node data"):
        data = list_output_data(ids.environment_id, ids.graph_id, ids.node_id, port)
    _print_objects(data, print_json)


def _print_objects(objects: list, print_json: bool):
    if not objects:
        if not print_json:
            sprint("[info]No data found")
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
