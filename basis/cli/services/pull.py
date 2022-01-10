from __future__ import annotations

from basis.cli.services.api import Endpoints, get


def download_graph_zip(graph_version_uid: str) -> bytes:
    resp = get(Endpoints.graph_version_download(graph_version_uid))
    resp.raise_for_status()
    return resp.content
