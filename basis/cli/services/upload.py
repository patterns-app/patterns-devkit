import base64
import os
from pathlib import Path

from basis.cli.helpers import compress_directory
from basis.cli.services.api import Endpoints, post
from basis.configuration.graph import NodeDefinitionCfg
from requests.models import Response

from basis.graph.builder import graph_as_configured_nodes


def upload_graph_version(
    cfg: NodeDefinitionCfg, pth_to_root: Path, organization_name: str
) -> dict:
    manifest = graph_as_configured_nodes(cfg, str(pth_to_root))
    zipf = compress_directory(pth_to_root)
    resp = post(
        Endpoints.GRAPH_VERSIONS_UPLOAD,
        data={
            "graph_name": cfg.name,
            "organization_name": organization_name,
            "graph_manifest": manifest.dict(exclude_unset=True),
        },
        files={"zip": zipf},
    )
    resp.raise_for_status()
    data = resp.json()
    return data
