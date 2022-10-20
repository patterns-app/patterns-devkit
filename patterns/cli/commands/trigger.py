from pathlib import Path

from typer import Option, Argument

from patterns.cli.commands._common import app_argument_help
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error, abort
from patterns.cli.services.trigger import trigger_node

_organization_help = (
    "The name of the Patterns organization that the graph specified "
    "with --app was uploaded to"
)
_node_id_help = "The id of the node to trigger"
_node_help = "The path to the node to trigger"


def trigger(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    app: str = Option(None, exists=True, help=app_argument_help),
    type: str = Option("pubsub", hidden=True),
    node_id: str = Option(None, help=_node_id_help),
    node: Path = Argument(None, exists=True, help=_node_help),
):
    """Trigger a node on an uploaded app to run immediately

    You can either pass a path to the node to trigger:

    patterns trigger ./app/my_node.py

    Or the id or slug of an app and the id of the node:

    patterns trigger --app=my-app --node-id=a1b2c3
    """
    if node and node_id:
        abort("Cannot specify both --node-id and NODE path argument")
    if node is None and node_id is None:
        abort("Must specify one of --node-id or NODE path argument")

    ids = IdLookup(
        organization_slug=organization,
        graph_slug_or_uid_or_path=app,
        node_file_path=node,
        node_id=node_id,
        find_nearest_graph=True,
    )
    with abort_on_error("Error triggering node"):
        trigger_node(
            ids.graph_uid,
            ids.node_id,
            execution_type=type,
        )

    sprint(f"[success]Triggered node {node}")
