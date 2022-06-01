from pathlib import Path

from typer import Option

from patterns.cli.services.deploy import deploy_graph_version
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error

_graph_help = "The location of the graph.yml file of the graph to deploy"
_graph_version_id_help = "The id of the graph version to deploy"
_environment_help = "The name of the Patterns environment deploy to"
_organization_help = "The name of the Patterns organization that the graph specified with --graph was uploaded to"


def deploy(
    environment: str = Option("", "-e", "--environment", help=_environment_help),
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    graph: Path = Option(None, help=_graph_help),
    graph_version_id: str = Option("", help=_graph_version_id_help),
):
    """Deploy a previously uploaded graph version

    You can specify either '--graph-version-id' to deploy a specific version, or
    '--graph' to deploy the latest uploaded version of a graph.
    """
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        explicit_graph_path=graph,
        explicit_graph_version_id=graph_version_id,
    )

    with abort_on_error("Deploy failed"):
        deploy_graph_version(ids.graph_version_id, ids.environment_id)

    sprint(f"[success]Graph deployed.")
