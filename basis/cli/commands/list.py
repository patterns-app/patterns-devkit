import json
from enum import Enum
from pathlib import Path
from typing import Iterable

import typer
from rich.table import Table
from typer import Option, Argument

from basis.cli.services.environments import paginated_environments
from basis.cli.services.list import (
    paginated_output_data,
    paginated_execution_events,
    paginated_graphs,
    paginated_webhook_urls, paginated_graph_components_regular,
    paginated_graph_components_admin,
)
from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, abort_on_error
from basis.graph.configured_node import NodeType


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
        gs = paginated_graphs(ids.organization_id).list()
    _print_objects(gs, print_json)


@list_command.command()
def environments(
    organization: str = Option("", "--organization", "-o", help=_organization_help),
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List environments"""
    ids = IdLookup(organization_name=organization)
    with abort_on_error("Error listing environments"):
        es = paginated_environments(ids.organization_id).list()
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
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        node_file_path=node,
    )

    with abort_on_error("Could not list logs"):
        events = paginated_execution_events(
            ids.environment_id, ids.graph_id, ids.node_id
        ).list()
    _print_objects(events, print_json)


_port_help = "The name of the output port"


@list_command.command()
def output(
    print_json: bool = Option(False, "--json", help=_json_help),
    organization: str = Option("", "--organization", "-o", help=_organization_help),
    environment: str = Option("", "--environment", "-e", help=_environment_help),
    node: Path = Argument(..., exists=True, help=_node_help),
    port: str = Argument(..., help=_port_help),
):
    """List data sent to an output port of a from the most recent run of a node"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        node_file_path=node,
    )

    with abort_on_error("Could not get node data"):
        data = paginated_output_data(
            ids.environment_id, ids.graph_id, ids.node_id, port
        ).list()
    _print_objects(data, print_json)


_graph_help = "The location of the graph.yml file of the graph to list"


@list_command.command()
def webhooks(
    print_json: bool = Option(False, "--json", help=_json_help),
    organization: str = Option("", "--organization", "-o", help=_organization_help),
    environment: str = Option("", "--environment", "-e", help=_environment_help),
    graph: Path = Argument(None, exists=True, help=_graph_help),
):
    """List webhook urls for a graph"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        explicit_graph_path=graph,
    )
    with abort_on_error("Could not get webhook data"):
        data = paginated_webhook_urls(ids.environment_id, ids.graph_id).list()

    # Add node names to output
    node_names_by_id = {n.id: n.name for n in ids.manifest.nodes}
    for d in data:
        d["name"] = node_names_by_id.get(d.get("node_id"), "")

    # URLs only exist for deployed webhooks, so add rows for undeployed nodes
    data_node_ids = {d.get("node_id") for d in data}
    for node in ids.manifest.nodes:
        if node.node_type == NodeType.Webhook and str(node.id) not in data_node_ids:
            data.append({"name": node.name, "node_id": node.id, "webhook_url": ""})

    _print_objects(data, print_json, headers=["name", "node_id", "webhook_url"])


@list_command.command()
def components(
    print_json: bool = Option(False, "--json", help=_json_help),
):
    """List available graph components that you can add to your graphs"""
    with abort_on_error("Could not get components"):
        data = paginated_graph_components_admin().list()
        data += paginated_graph_components_regular().list()

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
