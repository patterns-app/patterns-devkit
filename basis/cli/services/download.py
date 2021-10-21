import base64
import os
from pathlib import Path

from requests.models import Response

from basis.cli.helpers import compress_directory
from basis.cli.services.api import Endpoints, post
from basis.configuration.graph import GraphCfg

from basis.graph.builder import ConfiguredGraphBuilder


def download_graph_version(name: str, organization_uid: str) -> Response:
    manifest = ConfiguredGraphBuilder(
        directory=pth_to_root, cfg=cfg
    ).build_manifest_from_config()
    zipf = compress_directory(pth_to_root)
    b64_zipf = base64.b64encode(zipf.read())
    resp = post(
        Endpoints.GRAPH_VERSIONS_UPLOAD,
        data={"graph_manifest": manifest, "zip": b64_zipf.decode()},
    )
    return resp
