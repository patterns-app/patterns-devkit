import json
from enum import Enum
from pathlib import Path
from typing import Iterable

import typer
from rich.table import Table
from typer import Option, Argument

from patterns.cli.services.environments import paginated_environments
from patterns.cli.services.list import (
    paginated_output_data,
    paginated_execution_events,
    paginated_graphs,
    paginated_webhook_urls,
    paginated_graph_components,
)
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.organizations import paginated_organizations
from patterns.cli.services.output import sprint, abort_on_error


class Listable(str, Enum):
    env = "environments"
    graph = "graphs"


_type_help = "The type of object to list"
_json_help = "Output the object as JSON Lines"
_organization_help = "The name of the Patterns organization to use"
_environment_help = "The name of the Patterns environment to use"

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
        gs = list(paginated_graphs(ids.organization_id))
    _print_objects(gs, print_json)


@list_command.command()
def environments(
    organization: str = Option("", "--organization", "-o", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List environments"""
    ids = IdLookup(organization_name=organization)
    with abort_on_error("Error listing environments"):
        es = list(paginated_environments(ids.organization_id))
    _print_objects(es, print_json)


@list_command.command()
def organizations(
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List organizations"""
    with abort_on_error("Error listing organizations"):
        es = list(paginated_organizations())
    _print_objects(es, print_json)


_node_help = "The path to the node to get logs for"


@list_command.command()
def logs(
    print_json: bool = Option(False, "--json", help=_json_help),
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    environment: str = Option("", "-e", "--environment", help=_environment_help),
    node: Path = Argument(..., exists=True, help=_node_help),
):
    """List execution logs for a node"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        node_file_path=node,
    )

    with abort_on_error("Could not list logs"):
        events = list(
            paginated_execution_events(ids.environment_id, ids.graph_id, ids.node_id)
        )
    _print_objects(events, print_json)


_port_help = "The name of the output port"

_store_name_help = "The name of the store to get the latest output for"
_graph_help = "The location of the graph.yml file for the graph"


@list_command.command()
def output(
    print_json: bool = Option(False, "--json", help=_json_help),
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    environment: str = Option("", "-e", "--environment", help=_environment_help),
    graph: Path = Option("", "-g", "--graph-path", help=_graph_help),
    store_name: str = Argument(..., exists=True, help=_store_name_help),
):
    """List data sent to an output port of a from the most recent run of a node"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        explicit_graph_path=graph,
    )

    with abort_on_error("Could not get node data"):
        data = list(
            paginated_output_data(ids.environment_id, ids.graph_id, store_name)
        )
    _print_objects(data, print_json)


_graph_help = "The location of the graph.yml file of the graph to list"


@list_command.command()
def webhooks(
    print_json: bool = Option(False, "--json", help=_json_help),
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    environment: str = Option("", "-e", "--environment", help=_environment_help),
    graph: Path = Argument(None, exists=True, help=_graph_help),
):
    """List webhook urls for a graph"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        explicit_graph_path=graph,
    )
    with abort_on_error("Could not get webhook data"):
        data = list(paginated_webhook_urls(ids.environment_id, ids.graph_id))

    _print_objects(data, print_json, headers=["name", "node_id", "webhook_url"])


@list_command.command()
def components(print_json: bool = Option(False, "--json", help=_json_help)):
    """List available graph components that you can add to your graphs"""
    with abort_on_error("Could not get components"):
        data = list(paginated_graph_components())

    # flatten the latest_version dict
    for row in data:
        version = row.pop("latest_version", {})
        for k, v in version.items():
            if isinstance(v, dict) and "slug" in v:
                v = v["slug"]
            if k not in row:
                row[k] = v
        row["version_names"].remove(row["graph_version_uid"])

    _print_objects(data, print_json)


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
