import os
from pathlib import Path
from typing import Optional

from basis.cli.services.output import prompt_path


def resolve_graph_path(
    path: Path, exists: bool, create_parents_if_necessary: bool = True
) -> Path:
    """Resolve an explicitly given graph location to a yaml"""
    if path.is_dir():
        f = path / "graph.yml"
        if f.is_file():
            if exists:
                return f.absolute()
            raise ValueError(f"File '{f}' already exists")
        if exists:
            raise ValueError(f"File '{f}' does not exist")
        return f.absolute()
    if path.suffix and path.name != "graph.yml":
        raise ValueError(f"Invalid graph file name: {path.name}")
    if path.is_file():
        if not exists:
            raise ValueError(f"Graph '{path}' already exists")
        return path.absolute()
    if exists:
        raise ValueError(f"Graph '{path}' does not exist")
    if path.suffix:
        if create_parents_if_necessary:
            path.parent.mkdir(parents=True)
        return path.absolute()
    if create_parents_if_necessary:
        path.mkdir(parents=True)
    graph_path = (path / "graph.yml").absolute()
    return graph_path


def find_graph_file(path: Optional[Path], prompt: bool = True) -> Path:
    """Walk up a directory tree looking for a graph"""
    if path and path.is_file():
        return resolve_graph_path(path, exists=True)
    if not path:
        path = Path(os.getcwd())
    path = path.absolute()

    for _ in range(100):
        p = path / "graph.yml"
        if p.is_file():
            return p
        if not path or path == path.parent:
            break
        path = path.parent

    if prompt:
        resp = prompt_path("Enter the path to the graph yaml file", exists=True)
        return resolve_graph_path(resp, exists=True)
    else:
        raise ValueError(f"Cannot find graph.yml{f' at {path}' if path else ''}")
