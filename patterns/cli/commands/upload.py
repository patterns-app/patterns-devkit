import io
from pathlib import Path
from zipfile import ZipFile

import typer
from requests import HTTPError
from typer import Option, Argument

from patterns.cli.commands._common import app_argument_help
from patterns.cli.services.diffs import get_diffs_between_zip_and_dir, print_diffs
from patterns.cli.services.download import download_graph_zip
from patterns.cli.services.graph_components import create_graph_component
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error
from patterns.cli.services.upload import upload_graph_version

_app_help = "The location of the graph.yml file of the app to upload"
_organization_help = "The name of the Patterns organization to upload to"
_component_help = "After uploading, publish the app version as a public component"
_force_help = "Overwrite existing files without prompting"
_diff_help = "Show a full diff of file conflicts"


def upload(
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    force: bool = Option(False, "-f", "--force", help=_force_help),
    diff: bool = Option(False, "-d", "--diff", help=_diff_help),
    publish_component: bool = Option(False, help=_component_help),
    app: Path = Argument(None, exists=True, help=_app_help),
):
    """Upload a new version of an app to Patterns

    This command will never overwrite data by default. You can call this command with
    [bold cyan]--force[/] to overwrite files Patterns Studio.
    """
    ids = IdLookup(
        organization_slug=organization,
        graph_path=app,
    )

    if not force:
        try:
            content = io.BytesIO(download_graph_zip(ids.graph_version_uid))
        except HTTPError:
            # No graph version yet
            pass
        else:
            with ZipFile(content, "r") as zf:
                conflicts = get_diffs_between_zip_and_dir(zf, ids.graph_directory)
                if conflicts.is_not_empty:
                    sprint("[info]Upload would change the following files:\n")
                    print_diffs(conflicts, diff, True)
                    msg = "\n[info]Run this command with [code]--force[/code] to upload the app"
                    if not diff:
                        msg += ", or [code]--diff[/code] to see detailed differences"
                    sprint(msg)
                    raise typer.Exit(1)

    with abort_on_error("Upload failed"):
        resp = upload_graph_version(
            ids.graph_file_path,
            ids.organization_uid,
            add_missing_node_ids=not publish_component,
        )

    graph_version_id = resp["uid"]
    ui_url = resp["ui_url"]
    sprint(f"\n[success]Uploaded new app version with id [b]{graph_version_id}")
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
        sprint(f"[error]App contains the following errors:")
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
                f"[success]Published app component "
                f"[b]{resp_org}/{resp_component}[/b] "
                f"with version [b]{resp_version}[/b] "
                f"at id [b]{resp_id}"
            )

    sprint(f"\n[info]Visit [code]{ui_url}[/code] to view your app")
