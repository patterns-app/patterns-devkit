from pathlib import Path

from typer import Option, Argument

from patterns.cli.services.graph_components import create_graph_component
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error
from patterns.cli.services.upload import upload_graph_version

_graph_help = "The location of the graph.yml file for the graph to upload"
_organization_help = "The name of the Patterns organization to upload to"
_component_help = "After uploading, publish the graph version as a public component"
_slug_help = "The slug to use for the graph. Can only contain letters and hyphens. " \
             "Defaults to the name of the graph directory"

def upload(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    slug: str = Option(None, "--slug", help=_slug_help),
    graph: Path = Argument(None, exists=True, help=_graph_help),
    publish_component: bool = Option(False, help=_component_help),
):
    """Upload a new version of a graph to Patterns"""
    ids = IdLookup(
        organization_name=organization,
        graph_path=graph,
    )

    with abort_on_error("Upload failed"):
        resp = upload_graph_version(
            ids.graph_file_path,
            ids.organization_id,
            add_missing_node_ids=not publish_component,
            slug=slug
        )

    graph_version_id = resp["uid"]
    ui_url = resp["ui_url"]
    sprint(f"\n[success]Uploaded new graph version with id [b]{graph_version_id}")
    errors = resp.get("errors", [])
    if publish_component:
        errors = [
            e
            for e in errors
            if not e["message"].startswith("Top level input is not connected")
            and not (
                e["message"].startswith("Parameter")
                and e["message"].endswith("has no default or value")
            )
        ]
    if errors:
        sprint(f"[error]Graph contains the following errors:")
        for error in errors:
            sprint(f"\t[error]{error}")

    if publish_component:
        with abort_on_error("Error creating component"):
            resp = create_graph_component(graph_version_id)
            resp_org = resp["organization"]["slug"]
            resp_version = resp["version_name"]
            resp_component = resp["component"]["slug"]
            resp_id = resp["uid"]
            sprint(
                f"[success]Published graph component "
                f"[b]{resp_org}/{resp_component}[/b] "
                f"with version [b]{resp_version}[/b] "
                f"at id [b]{resp_id}"
            )

    sprint(f"\n[info]Visit [code]{ui_url}[/code] to view your graph")
