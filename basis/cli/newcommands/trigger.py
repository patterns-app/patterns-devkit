from pathlib import Path

from typer import Option, Argument

from basis.cli.config import read_local_basis_config
from basis.cli.newapp import app
from basis.cli.newcommands._util import _get_graph_version_id, _get_graph_path
from basis.cli.services.api import abort_on_http_error
from basis.cli.services.output import abort
from basis.cli.services.trigger import trigger_node
from basis.graph.builder import graph_manifest_from_yaml
from basis.graph.configured_node import GraphManifest

_graph_help = "The location of the graph.yml file for the deployed graph"
_graph_version_id_help = "The id of the deployed graph version"
_environment_help = "The name of the Basis environment that the graph is deployed to"
_organization_help = "The name of the Basis organization that the graph specified with --graph was uploaded to"
_node_help = "The path to the node to trigger"


@app.command()
def trigger(
    organization: str = Option("", help=_organization_help),
    environment: str = Option("", help=_environment_help),
    graph: Path = Option(None, exists=True, help=_graph_help),
    graph_version_id: str = Option("", help=_graph_version_id_help),
    node: Path = Argument(..., exists=True, help=_node_help),
):
    """Trigger a node on a deployed graph to run immediately"""
    cfg = read_local_basis_config()
    graph_path = _get_graph_path(cfg, graph)

    manifest = graph_manifest_from_yaml(graph_path)
    node_id = _get_node_id(graph_path, manifest, node)
    if not node_id:
        abort(f"Node {node} is not part of the graph at {graph_path}")

    graph_version_id = _get_graph_version_id(cfg, graph, graph_version_id, organization)

    with abort_on_http_error("Error triggering node"):
        trigger_node(node_id, graph_version_id, environment or cfg.environment_name)


def _get_node_id(graph_path: Path, manifest: GraphManifest, node: Path):
    try:
        node_path = node.absolute().relative_to(graph_path.parent)
    except Exception:
        return None
    node_id = next(
        (
            n.id
            for n in manifest.nodes
            if Path(n.file_path_to_node_script_relative_to_root) == node_path
        ),
        None,
    )
    return node_id
