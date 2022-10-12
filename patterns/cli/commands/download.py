import io
from pathlib import Path
from zipfile import ZipFile

import typer
from typer import Option, Argument

from patterns.cli.services.download import (
    download_graph_zip,
)
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error

_app_help = "The slug or uid of a app or app version"
_directory_help = "The directory to download the app to"
_organization_help = "The name of the Patterns organization that the graph belongs to"
_force_help = "Overwrite existing files without prompting"


def download(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    force: bool = Option(False, "-f", "--force", help=_force_help),
    app: str = Argument(None, help=_app_help),
    directory: Path = Argument(None, help=_directory_help, file_okay=False),
):
    """Download the code for a Patterns app"""
    ids = IdLookup(
        organization_name=organization, graph_slug_or_uid=app, graph_path=directory
    )

    with abort_on_error("Error downloading app"):
        content = io.BytesIO(download_graph_zip(ids.graph_version_uid))

        root = (
            directory
            if directory
            # If a graph is specified, download it to a folder matching its slug.
            else Path(ids.graph_slug).resolve()
            if app
            # Otherwise download the current graph
            else ids.graph_directory
        )
        with ZipFile(content, "r") as zf:
            if force:
                zf.extractall(root)
            else:
                conflicts = _get_conflicts(zf, root)
                if conflicts:
                    sprint("[error]Download would overwrite the following files:")
                    for conflict in conflicts:
                        sprint(f"\t[error]{conflict}")
                    sprint(
                        "[info]Run this command with --force to overwrite local files"
                    )
                    raise typer.Exit(1)
                else:
                    zf.extractall(root)

    sprint(f"[success]Downloaded app {ids.graph_slug}")


def _get_conflicts(zf: ZipFile, root: Path) -> list[str]:
    conflicts = []
    for zipinfo in zf.infolist():
        dst = root / zipinfo.filename
        if zipinfo.is_dir() or not dst.exists():
            continue
        if dst.is_file():
            new = zf.read(zipinfo).decode()
            if dst.read_text() != new:
                conflicts.append(zipinfo.filename)
        else:
            conflicts.append(zipinfo.filename)
    return conflicts
