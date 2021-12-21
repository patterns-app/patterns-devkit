import os
from pathlib import Path
from typing import Optional

from basis.cli.services.output import abort
from basis.cli.services.output import prompt_path


def resolve_graph_path(path: Path, exists: bool) -> Path:
    """Resolve an explicitly given graph location to a yaml"""
    if path.is_dir():
        for ext in (".yml", ".yaml"):
            f = path / f"graph{ext}"
            if f.is_file():
                if exists:
                    return f.absolute()
                abort(f"Graph '{f}' already exists")
        if exists:
            abort(f"Graph '{f}' does not exist")
        return (path / "graph.yml").absolute()
    if path.suffix and path.suffix not in (".yml", ".yaml"):
        abort(f"Graph '{path}' must be a yaml file")
    if path.is_file():
        if not exists:
            abort(f"Graph '{path}' already exists")
        return path.absolute()
    if exists:
        abort(f"Graph '{path}' does not exist")
    if path.suffix:
        return path.absolute()
    path.mkdir(parents=True)
    graph_path = (path / "graph.yml").absolute()
    return graph_path


def find_graph_file(path: Optional[Path]) -> Path:
    """Walk up a directory tree looking for a graph"""
    if path and path.is_file():
        return resolve_graph_path(path, exists=True)
    if not path:
        path = Path(os.getcwd())
    path = path.absolute()

    for _ in range(100):
        for ext in ("yml", "yaml"):
            p = path / f"graph.{ext}"
            if p.is_file():
                return p
        if not path or path == path.parent:
            break
        path = path.parent

    resp = prompt_path("Enter the path to the graph yaml file", exists=True)
    return resolve_graph_path(resp, exists=True)
