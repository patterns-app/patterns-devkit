from pathlib import Path

from typer import Option

from basis.cli.config import (
    read_local_basis_config,
    resolve_graph_path,
    write_local_basis_config,
)
from basis.cli.newapp import app

_config_help = "The name of the organization to use by default"
_environment_help = "The name of the organization to use by default"
_graph_help = "The path to the graph to use by default"


@app.command()
def config(
    organization: str = Option("", "--organization", "-o", help=_config_help),
    environment: str = Option("", "--environment", "-e", help=_environment_help),
    graph: Path = Option(None, "--graph", "-g", exists=True, help=_graph_help),
):
    """Change the default values used by other commands"""
    cfg = read_local_basis_config()
    if organization:
        cfg.organization_name = organization
    if environment:
        cfg.environment_name = environment
    if graph:
        cfg.default_graph = resolve_graph_path(graph, exists=True)
    write_local_basis_config(cfg)
