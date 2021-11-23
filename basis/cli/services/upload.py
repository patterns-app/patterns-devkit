import json
from pathlib import Path

from basis.cli.helpers import compress_directory
from basis.cli.services.api import Endpoints, post
from basis.graph.builder import graph_manifest_from_yaml


def upload_graph_version(graph_yaml_path: Path, organization_name: str) -> dict:
    manifest = graph_manifest_from_yaml(graph_yaml_path)
    pth_to_root = graph_yaml_path.parent
    zipf = compress_directory(pth_to_root)
    resp = post(
        Endpoints.GRAPH_VERSIONS_CREATE,
        params={
            "graph_name": manifest.graph_name,
            "organization_name": organization_name,
        },
        data={
            "payload": json.dumps(
                {"manifest": json.dumps(manifest.dict(exclude_none=True))}
            ),
        },
        files={
            "file": zipf,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    return data
