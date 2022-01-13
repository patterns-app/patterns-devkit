from pathlib import Path

from typer import Option, Argument

from basis.cli.services.deploy import deploy_graph_version
from basis.cli.services.graph_components import create_graph_component
from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, abort_on_error, abort
from basis.cli.services.upload import upload_graph_version

_graph_help = "The location of the graph.yml file for the graph to upload"
_deploy_help = "Whether or not to automatically deploy the graph after upload"
_organization_help = "The name of the Basis organization to upload to"
_environment_help = "The name of the Basis environment to use if deploying the graph"
_component_path_help = "Publish the given node or subgraph as a public component"
_component_name_help = (
    "The name of the component published with --component-path [default: node name]"
)
_component_desc_help = (
    "The description of the component published with --component-path"
)


def upload(
    deploy: bool = Option(True, "--deploy/--no-deploy", help=_deploy_help),
    organization: str = Option("", help=_organization_help),
    environment: str = Option("", help=_environment_help),
    component_path: Path = Option(
        None, exists=True, dir_okay=False, help=_component_path_help
    ),
    component_name: str = Option("", help=_component_name_help),
    component_description: str = Option("", help=_component_desc_help),
    graph: Path = Argument(None, exists=True, help=_graph_help),
):
    """Upload a new version of a graph to Basis"""
    ids = IdLookup(
        environment_name=environment,
        organization_name=organization,
        explicit_graph_path=graph,
    )
    # validate component path before upload
    if component_path:
        with abort_on_error("Error creating component"):
            rel_path = "/".join(
                component_path.absolute().relative_to(ids.graph_file_path.parent).parts
            )
            if rel_path not in (
                n.file_path_to_node_script_relative_to_root for n in ids.manifest.nodes
            ):
                abort("Component file not part of graph")

    with abort_on_error("Upload failed"):
        resp = upload_graph_version(ids.graph_file_path, ids.organization_id)
    graph_version_id = resp["uid"]
    graph_name = resp["graph"]["name"]
    ui_url = resp["ui_url"]
    sprint(f"\n[success]Uploaded new graph version with id [b]{graph_version_id}.")

    if deploy:
        with abort_on_error("Deploy failed"):
            deploy_graph_version(graph_version_id, ids.environment_id)
        sprint(f"[success]Graph deployed.")

    if component_path:
        with abort_on_error("Error creating component"):
            name = component_name or (
                graph_name if component_path.stem == "graph" else component_path.stem
            )
            resp = create_graph_component(
                name, rel_path, graph_version_id, component_description,
            )
            resp_name = resp["name"]
            resp_id = resp["uid"]
            sprint(
                f"[success]Published graph component [b]{resp_name}[/b] "
                f"with id [b]{resp_id}"
            )

    sprint(f"\n[info]Visit [code]{ui_url}[/code] to view your graph")
