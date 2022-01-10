import json
import sys
from pathlib import Path

from typer import Option, Argument

from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import console
from basis.graph.builder import graph_manifest_from_yaml

_pretty_help = "Indent JSON output"
_graph_help = "The location of the graph.yml file"


def manifest(
    pretty: bool = Option(
        sys.stdout.isatty(), "--pretty/--no-pretty", help=_pretty_help
    ),
    graph: Path = Argument(None, exists=True, help=_graph_help),
):
    """Print a graph manifest as JSON"""
    ids = IdLookup(explicit_graph_path=graph)
    m = graph_manifest_from_yaml(ids.graph_file_path, allow_errors=True)
    j = json.dumps(m.dict(exclude_none=True))
    if pretty:
        console.print_json(j)
    else:
        print(j)
