import json
import sys
from pathlib import Path

from typer import Option, Argument

from basis.cli.config import read_local_basis_config
from basis.cli.services.graph import get_graph_path
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
    cfg = read_local_basis_config()
    graph_path = get_graph_path(cfg, graph)

    m = graph_manifest_from_yaml(graph_path, allow_errors=True)
    j = json.dumps(m.dict(exclude_none=True))
    if pretty:
        console.print_json(j)
    else:
        print(j)
