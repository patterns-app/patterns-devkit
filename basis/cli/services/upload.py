import json
from pathlib import Path

from basis.cli.services.api import Endpoints, post_for_json
from basis.configuration.edit import GraphDirectoryEditor


def upload_graph_version(graph_yaml_path: Path, organization_uid: str) -> dict:
    editor = GraphDirectoryEditor(graph_yaml_path).add_missing_node_ids()
    return post_for_json(
        Endpoints.graph_version_create(organization_uid),
        data={"payload": json.dumps({"graph_name": editor.graph_name()})},
        files={"file": editor.compress_directory()},
    )
