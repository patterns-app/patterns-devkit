import os
from pathlib import Path

from typer import Option

from basis.cli.config import (
    read_local_basis_config,
    resolve_graph_path,
)
from basis.cli.newapp import app
from basis.cli.services.api import abort_on_http_error
from basis.cli.services.deploy import deploy_graph_version
from basis.cli.services.graph_versions import get_latest_graph_version
from basis.cli.services.output import print_success, abort
from basis.configuration.base import load_yaml

_graph_help = "The location of the graph.yml file of the graph to deploy"
_graph_version_id_help = "The id of the graph version to deploy"
_environment_help = "The name of the Basis environment deploy to"
_organization_help = "The name of the Basis organization that the graph specified with --graph was uploaded to"


@app.command()
def deploy(
    environment: str = Option("", "-e", "--environment", help=_environment_help),
    organization: str = Option("", help=_organization_help),
    graph: Path = Option("", exists=True, help=_graph_help),
    graph_version_id: str = Option(""),
):
    """Deploy a previously uploaded graph version

    You can specify either '--graph-version-id' to deploy a specific version, or
    '--graph' to deploy the latest uploaded version of a graph.
    """
    cfg = read_local_basis_config()
    cwd = Path(os.getcwd())
    if not graph_version_id:
        if graph:
            graph_path = resolve_graph_path(graph, exists=True)
        elif cwd.is_relative_to(cfg.default_graph):
            graph_path = cfg.default_graph
        else:
            abort("You must specify either --graph or --graph-version-id")
        yaml = load_yaml(graph_path)
        graph_name = yaml.get("name", graph_path.parent.name)
        with abort_on_http_error("Retrieving graph version failed"):
            resp = get_latest_graph_version(
                graph_name, organization or cfg.organization_name
            )
        graph_version_id = resp["uid"]

    with abort_on_http_error("Deploy failed"):
        resp = deploy_graph_version(
            graph_version_id, environment or cfg.environment_name
        )

    print_success(f"Graph {resp['graph_name']} deployed.")
