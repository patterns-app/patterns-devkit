from pathlib import Path

from typer import Option

from basis.cli.config import read_local_basis_config
from basis.cli.services.deploy import deploy_graph_version
from basis.cli.services.graph import get_graph_version_id
from basis.cli.services.output import sprint, abort_on_error

_graph_help = "The location of the graph.yml file of the graph to deploy"
_graph_version_id_help = "The id of the graph version to deploy"
_environment_help = "The name of the Basis environment deploy to"
_organization_help = "The name of the Basis organization that the graph specified with --graph was uploaded to"


def deploy(
    environment: str = Option("", "-e", "--environment", help=_environment_help),
    organization: str = Option("", help=_organization_help),
    graph: Path = Option(None, help=_graph_help),
    graph_version_id: str = Option(""),
):
    """Deploy a previously uploaded graph version

    You can specify either '--graph-version-id' to deploy a specific version, or
    '--graph' to deploy the latest uploaded version of a graph.
    """
    cfg = read_local_basis_config()
    graph_version_id = get_graph_version_id(cfg, graph, graph_version_id, organization)

    with abort_on_error("Deploy failed"):
        resp = deploy_graph_version(
            graph_version_id, environment or cfg.environment_name
        )

    sprint(f"[success]Graph [code]{resp['graph_name']}[/code] deployed.")
