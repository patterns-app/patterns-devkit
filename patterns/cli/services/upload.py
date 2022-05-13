import json
from pathlib import Path

from patterns.cli.services.api import Endpoints, post_for_json
from patterns.configuration.edit import GraphDirectoryEditor


def upload_graph_version(
    graph_yaml_path: Path, organization_uid: str, add_missing_node_ids: bool
) -> dict:
    editor = GraphDirectoryEditor(graph_yaml_path)
    if add_missing_node_ids:
        editor.add_missing_node_ids()
    payload = {"slug": editor.graph_slug(), "root_yaml_path": editor.yml_path.name}
    return post_for_json(
        Endpoints.graph_version_create(organization_uid),
        data={"payload": json.dumps(payload)},
        files={"file": editor.compress_directory()},
    )
