import io
import re
from pathlib import Path
from zipfile import ZipFile

import typer
from typer import Option, Argument

from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error, abort
from patterns.cli.services.pull import (
    download_graph_zip,
    download_component_zip,
    COMPONENT_RE,
)
from patterns.configuration.edit import GraphDirectoryEditor, FileOverwriteError

_graph_help = "The name of a graph in your Patterns organization [default: directory name]"
_graph_version_id_help = (
    "The id of the graph version to pull. [default: latest version]"
)
_organization_help = "The name of the Patterns organization that the graph was uploaded to"
_force_help = "Overwrite existing files without prompting"
_directory_help = "The directory to create the new graph in. Must not exist."
_component_help = (
    "The component version to download (e.g. 'organization/component@v1')."
)


def clone(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    graph: str = Option("", help=_graph_help),
    graph_version_id: str = Option("", "-v", "--version", help=_graph_version_id_help),
    component: str = Option("", "--component", help=_component_help),
    directory: Path = Argument(None, exists=False, help=_graph_help),
):
    """Download the code for a graph"""
    if not graph and not directory and not component:
        if graph_version_id:
            abort(
                f"Missing graph directory argument."
                f"\ntry [code]patterns clone -v {graph_version_id} new_graph"
            )
        else:
            abort(
                f"Missing graph argument." f"\ntry [code]patterns clone graph-to-clone"
            )
    component_match = COMPONENT_RE.fullmatch(component)
    if component and not component_match:
        abort(
            "Invalid component version. Must be in the form organization/component@v1"
        )

    component_name = component_match.group(2) if component_match else None

    ids = IdLookup(
        organization_name=organization,
        explicit_graph_name=graph or component_name or directory.name,
        explicit_graph_version_id=graph_version_id,
    )
    if not directory:
        if component:
            directory = Path(component_name)
        elif graph:
            directory = Path(graph)
        elif graph_version_id:
            with abort_on_error("Error"):
                directory = Path(ids.graph_name)
        else:
            abort("Specify --graph, --graph-version-id, or a directory")

    with abort_on_error("Error cloning graph"):
        if component:
            content = download_component_zip(component)
        else:
            content = download_graph_zip(ids.graph_version_id)
        editor = GraphDirectoryEditor(directory, overwrite=False)
        with ZipFile(io.BytesIO(content), "r") as zf:
            editor.add_node_from_zip("graph.yml", "graph.yml", zf)

    sprint(f"[success]Cloned graph into {directory}")


_pull_graph_help = "The location of the graph to pull into [default: current directory]"


def pull(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    graph_version_id: str = Option("", help=_graph_version_id_help),
    force: bool = Option(False, "-f", "--force", help=_force_help),
    graph: Path = Argument(None, exists=True, help=_pull_graph_help),
):
    """Update the code for the current graph"""
    ids = IdLookup(
        organization_name=organization,
        explicit_graph_version_id=graph_version_id,
        explicit_graph_path=graph,
    )
    with abort_on_error("Error downloading graph"):
        b = io.BytesIO(download_graph_zip(ids.graph_version_id))
        editor = GraphDirectoryEditor(ids.graph_file_path, overwrite=force)

    with abort_on_error("Error downloading graph"):
        try:
            with ZipFile(b, "r") as zf:
                editor.add_node_from_zip("graph.yml", "graph.yml", zf)
        except FileOverwriteError as e:
            sprint(f"[error]{e}")
            sprint("[info]Run this command with --force to overwrite local files")
            raise typer.Exit(1)
    sprint(f"[success]Pulled graph content")
