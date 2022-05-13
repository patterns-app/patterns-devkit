from pathlib import Path


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
    if path.suffix and path.suffix not in (".yml", ".yaml"):
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

