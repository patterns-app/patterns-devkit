import io
from pathlib import Path
from zipfile import ZipFile

import typer
from typer import Option, Argument

from patterns.cli.commands._common import app_argument_help
from patterns.cli.services.diffs import get_diffs_between_zip_and_dir, print_diffs
from patterns.cli.services.download import (
    download_graph_zip,
)
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error

_directory_help = "The directory to download the app to"
_organization_help = "The name of the Patterns organization that the graph belongs to"
_force_help = "Overwrite existing files without prompting"
_diff_help = "Show a full diff of file conflicts"


def download(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    force: bool = Option(False, "-f", "--force", help=_force_help),
    diff: bool = Option(False, "-d", "--diff", help=_diff_help),
    app: str = Argument(None, help=app_argument_help),
    directory: Path = Argument(None, help=_directory_help, file_okay=False),
):
    """Download the code for a Patterns app

    Call this command like [bold cyan]patterns download my-app[/] to download the app named "my-app"
    to a new folder.

    If you are in the directory of an app you've already downloaded, you can get the
    latest version of the app by calling [bold cyan]patterns download[/] with no extra arguments.

    This command will never overwrite data by default. You can call this command with
    [bold cyan]--force[/] to overwrite local files.

    This command will never delete files, no matter if they're part of the app or not.
    """
    ids = IdLookup(
        organization_slug=organization, graph_slug_or_uid_or_path=app
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
                conflicts = get_diffs_between_zip_and_dir(zf, root)
                if not conflicts.changed:
                    zf.extractall(root)
                    sprint(f"[success]Downloaded app {ids.graph_slug}")
                    return
                sprint("[error]Download would overwrite the following files:\n")
                print_diffs(conflicts, diff, False)
                msg = "\n[info]Run this command with [code]--force[/code] to overwrite local files"
                if not diff:
                    msg += ", or [code]--diff[/code] to see detailed differences"
                sprint(msg)
                raise typer.Exit(1)
