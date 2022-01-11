import io
from pathlib import Path
from zipfile import ZipFile

import typer
from typer import Option, Argument

from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, abort_on_error, abort
from basis.cli.services.pull import download_graph_zip
from basis.configuration.edit import GraphDirectoryEditor, FileOverwriteError

_graph_help = "The name of a graph in your Basis organization [default: directory name]"
_graph_version_id_help = (
    "The id of the graph version to pull. [default: latest version]"
)
_organization_help = "The name of the Basis organization that the graph was uploaded to"
_force_help = "Overwrite existing files without prompting"
_directory_help = "The directory to create the new graph in. Must not exist."


def clone(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    graph: str = Option("", help=_graph_help),
    graph_version_id: str = Option("", help=_graph_version_id_help),
    directory: Path = Argument(None, exists=False, help=_graph_help),
):
    """Download the code for a graph"""
    ids = IdLookup(
        organization_name=organization,
        explicit_graph_name=graph or directory.name,
        explicit_graph_version_id=graph_version_id,
    )
    if not directory:
        if graph:
            directory = Path(graph)
        elif graph_version_id:
            with abort_on_error("Error"):
                directory = Path(ids.graph_name)
        else:
            abort("Specify --graph, --graph-version-id, or a directory")

    with abort_on_error("Error cloning graph"):
        b = io.BytesIO(download_graph_zip(ids.graph_version_id))
        editor = GraphDirectoryEditor(directory, overwrite=False)
        with ZipFile(b, "r") as zf:
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
