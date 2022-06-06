from pathlib import Path

from rich.prompt import Confirm
from typer import Option, Argument

from patterns.cli.services.delete import delete_graph
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error

_graph_help = "The location of the graph.yml file of the graph to delete"
_graph_id_help = "The id of the graph version to delete"
_force_help = "Don't prompt before deleting a graph"


def delete(
    graph_id: str = Option(""),
    force: bool = Option(False, "-f", "--force", help=_force_help),
    graph: Path = Argument(None, exists=True, help=_graph_help),
):
    """Delete a graph from the Patterns studio.

    This will not delete any files locally.
    """
    ids = IdLookup(
        explicit_graph_path=graph,
        explicit_graph_id=graph_id,
    )

    with abort_on_error("Deleting graph failed"):
        if not force:
            Confirm.ask(f"Delete graph {ids.graph_name}?")
        delete_graph(ids.graph_id)

    sprint(f"[success]Graph deleted from Patterns studio.")
