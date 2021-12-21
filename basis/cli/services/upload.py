import json
from pathlib import Path

from basis.cli.helpers import compress_directory
from basis.cli.services.api import Endpoints, post_for_json
from basis.graph.builder import graph_manifest_from_yaml


def upload_graph_version(graph_yaml_path: Path, organization_uid: str) -> dict:
    manifest = graph_manifest_from_yaml(graph_yaml_path)
    zipf = compress_directory(graph_yaml_path.parent)
    return post_for_json(
        Endpoints.graph_version_create(organization_uid),
        data={
            "payload": json.dumps(
                {
                    "graph_name": manifest.graph_name,
                    "manifest": manifest.dict(exclude_none=True),
                }
            )
        },
        files={"file": zipf},
    )
