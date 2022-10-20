from rich.prompt import Confirm
from typer import Option, Argument

from patterns.cli.commands._common import app_argument_help
from patterns.cli.services.delete import delete_graph
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error

_force_help = "Don't prompt before deleting an app"
_organization_help = "The name of the Patterns organization to delete from"


def delete(
    force: bool = Option(False, "-f", "--force", help=_force_help),
    organization: str = Option("", "-o", "--organization", help=_organization_help),
    app: str = Argument(None, help=app_argument_help),
):
    """Delete an app from the Patterns studio.

    This will not delete any files locally.
    """
    ids = IdLookup(organization_slug=organization, graph_slug_or_uid_or_path=app)

    with abort_on_error("Deleting app failed"):
        if not force:
            Confirm.ask(f"Delete app {ids.graph_slug}?")
        delete_graph(ids.graph_uid)

    sprint(f"[success]App deleted from Patterns studio.")
