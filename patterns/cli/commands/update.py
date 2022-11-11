from typing import Optional

import typer
from typer import Option

from patterns.cli.commands._common import app_argument
from patterns.cli.services.graph_components import update_graph_component
from patterns.cli.services.graph_versions import update_graph
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error

_organization_help = "The name of the Patterns organization to use"
_public_help = "Set the app to public or private. Public apps may be viewed by anyone."
_deprecated_help = "Set the app's component as deprecated or not. Deprecated apps cannot be added to new apps, but continue to function if existing apps use them"
_organization_option = Option("", "--organization", "-o", help=_organization_help)

update_command = typer.Typer(name="update", help="Update an object of a given type")


@update_command.command()
def app(
    organization: str = Option("", help=_organization_help),
    public: Optional[bool] = Option(
        None, "--public/--private", show_default=False, help=_public_help
    ),
    deprecated: Optional[bool] = Option(
        None, "--deprecated/--no-deprecated", show_default=False, help=_deprecated_help
    ),
    app_location: str = app_argument,
):
    """Update properties of an app"""
    ids = IdLookup(
        organization_slug=organization, graph_slug_or_uid_or_path=app_location
    )
    with abort_on_error("Error updating app"):
        if public is not None:
            update_graph(ids.graph_uid, public=public)
        if deprecated is not None:
            update_graph_component(ids.graph_uid, deprecated=deprecated)

    if public is not None or deprecated is not None:
        sprint("[success]Updated app successfully")
