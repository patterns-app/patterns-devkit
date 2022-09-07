from pathlib import Path

from typer import Option, Argument

from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error, abort
from patterns.cli.services.trigger import trigger_node

_graph_help = "The location of the graph.yml file for the deployed graph"
_graph_version_id_help = "The id of the deployed graph version"
_organization_help = (
    "The name of the Patterns organization that the graph specified "
    "with --graph was uploaded to"
)
_node_id_help = "The id of the node to trigger"
_node_help = "The path to the node to trigger"


def trigger(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    graph: Path = Option(None, exists=True, help=_graph_help),
    graph_version_id: str = Option("", help=_graph_version_id_help),
    type: str = Option("pubsub", hidden=True),
    node_id: str = Option(None, help=_node_id_help),
    node: Path = Argument(None, exists=True, help=_node_help),
):
    """Trigger a node on a deployed graph to run immediately

    You can either pass a path to the node to trigger:

    patterns trigger ./graph/my_node.py

    Or a path to the graph and the id of the node:

    patterns trigger --graph=./graph/my_node.py --node-id=a1b2c3
    """
    if node and node_id:
        abort("Cannot specify both --node-id and NODE path argument")
    if node is None and node_id is None:
        abort("Must specify one of --node-id or NODE path argument")

    ids = IdLookup(
        organization_name=organization,
        graph_path=graph,
        node_file_path=node,
        node_id=node_id,
        graph_version_id=graph_version_id,
        find_nearest_graph=True,
    )
    with abort_on_error("Error triggering node"):
        trigger_node(
            ids.graph_id,
            ids.node_id,
            execution_type=type,
        )

    sprint(f"[success]Triggered node {node}")
