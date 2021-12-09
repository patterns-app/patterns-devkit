import os
from pathlib import Path
from typing import Optional

from basis.cli.config import CliConfig
from basis.cli.config import resolve_graph_path
from basis.cli.services.api import abort_on_http_error
from basis.cli.services.graph_versions import get_latest_graph_version
from basis.cli.services.output import abort
from basis.cli.services.output import prompt_path
from basis.cli.services.paths import is_relative_to
from basis.configuration.base import load_yaml


def get_graph_version_id(
    cfg: CliConfig,
    graph: Optional[Path],
    graph_version_id: Optional[str],
    organization: Optional[str],
):
    if graph_version_id:
        return graph_version_id
    cwd = Path(os.getcwd())
    if graph:
        graph_path = resolve_graph_path(graph, exists=True)
    elif is_relative_to(cwd, cfg.default_graph.parent):
        graph_path = cfg.default_graph
    else:
        abort("You must specify either --graph or --graph-version-id")
    yaml = load_yaml(graph_path)
    graph_name = yaml.get("name", graph_path.parent.name)
    with abort_on_http_error("Retrieving graph version failed"):
        resp = get_latest_graph_version(
            graph_name, organization or cfg.organization_name
        )
    return resp["uid"]


def get_graph_path(cfg: CliConfig, graph: Optional[Path]):
    cwd = Path(os.getcwd()).absolute()
    if graph:
        if graph.is_dir():
            for ext in ('.yml', '.yaml',):
                p = graph / f'graph{ext}'
                if p.is_file():
                    return p
            abort(f'Could not find graph at {graph}')
        else:
            return graph

    if not graph and not is_relative_to(cwd, cfg.default_graph.parent) or not cfg.default_graph:
        prompt = "Enter the location of the graph.yml file"
        graph_path = prompt_path(prompt, exists=True).absolute()
        graph_path = resolve_graph_path(graph_path, exists=True)
    else:
        graph_path = cfg.default_graph
    return graph_path
