from pathlib import Path

from typer import Option, Argument

from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, abort_on_error
from basis.cli.services.trigger import trigger_node, TypeChoices

_graph_help = "The location of the graph.yml file for the deployed graph"
_graph_version_id_help = "The id of the deployed graph version"
_environment_help = "The name of the Basis environment that the graph is deployed to"
_organization_help = (
    "The name of the Basis organization that the graph specified "
    "with --graph was uploaded to"
)
_node_help = "The path to the node to trigger"


def trigger(
    organization: str = Option("", "-e", "--environment", help=_organization_help),
    environment: str = Option("", help=_environment_help),
    graph: Path = Option(None, exists=True, help=_graph_help),
    graph_version_id: str = Option("", help=_graph_version_id_help),
    type: TypeChoices = Option(TypeChoices.pubsub, hidden=True),
    node: Path = Argument(..., exists=True, help=_node_help),
):
    """Trigger a node on a deployed graph to run immediately"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        explicit_graph_path=graph,
        node_file_path=node,
        explicit_graph_version_id=graph_version_id,
    )
    with abort_on_error("Error triggering node"):
        trigger_node(
            ids.node_id, ids.graph_version_id, ids.environment_id, execution_type=type,
        )

    sprint(f"[success]Triggered node {node}")
