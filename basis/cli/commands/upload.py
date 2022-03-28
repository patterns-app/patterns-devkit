from pathlib import Path

from typer import Option, Argument

from basis.cli.services.deploy import deploy_graph_version
from basis.cli.services.graph_components import create_graph_component
from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, abort_on_error
from basis.cli.services.upload import upload_graph_version

_graph_help = "The location of the graph.yml file for the graph to upload"
_deploy_help = "Whether or not to automatically deploy the graph after upload"
_organization_help = "The name of the Basis organization to upload to"
_environment_help = "The name of the Basis environment to use if deploying the graph"
_component_help = "After uploading, publish the graph version as a public component"


def upload(
    deploy: bool = Option(True, "--deploy/--no-deploy", help=_deploy_help),
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    environment: str = Option("", "-e", "--environment", help=_environment_help),
    graph: Path = Argument(None, exists=True, help=_graph_help),
    publish_component: bool = Option(False, help=_component_help),
):
    """Upload a new version of a graph to Basis"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        explicit_graph_path=graph,
    )

    with abort_on_error("Upload failed"):
        resp = upload_graph_version(
            ids.graph_file_path,
            ids.organization_id,
            add_missing_node_ids=not publish_component,
        )

    graph_version_id = resp["uid"]
    ui_url = resp["ui_url"]
    manifest = resp["manifest"]
    sprint(f"\n[success]Uploaded new graph version with id [b]{graph_version_id}")
    if manifest.get("errors"):
        sprint(f"[error]Graph contains the following errors:")
        for error in manifest["errors"]:
            sprint(f"\t[error]{error}")

    if publish_component:
        with abort_on_error("Error creating component"):
            resp = create_graph_component(graph_version_id)
            resp_org = resp["organization"]["slug"]
            resp_versions = resp["version_names"]
            resp_component = resp["component"]["slug"]
            resp_id = resp["uid"]
            sprint(
                f"[success]Published graph component "
                f"[b]{resp_org}/{resp_component}[/b] "
                f"with versions [b]{resp_versions}[/b] "
                f"at id [b]{resp_id}"
            )
    elif deploy:
        with abort_on_error("Deploy failed"):
            deploy_graph_version(graph_version_id, ids.environment_id)
        sprint(f"[success]Graph deployed")

    sprint(f"\n[info]Visit [code]{ui_url}[/code] to view your graph")
