import base64
import os
from pathlib import Path
from typing import Dict

from basis.cli.helpers import compress_directory
from basis.cli.services.api import Endpoints, post
from basis.configuration.graph import GraphCfg
from basis.graph.builder import ConfiguredGraphBuilder
from requests.models import Response


def upload_graph_version(
    cfg: GraphCfg, pth_to_root: Path, organization_uid: str
) -> Dict:
    manifest = ConfiguredGraphBuilder(
        directory=pth_to_root, cfg=cfg
    ).build_manifest_from_config()
    zipf = compress_directory(pth_to_root)
    resp = post(
        Endpoints.GRAPH_VERSIONS_UPLOAD,
        data={
            "graph_name": cfg.name,
            "organization_uid": organization_uid,
            "graph_manifest": manifest.dict(exclude_unset=True),
        },
        files={"zip": zipf},
    )
    resp.raise_for_status()
    data = resp.json()
    return data
