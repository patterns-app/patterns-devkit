from __future__ import annotations

import re

from patterns.cli.services.api import Endpoints, get

COMPONENT_RE = re.compile(r"([\w\-]+)/([\w\d\-]+)@([\w\d\-.]+)")


def download_graph_zip(graph_version_uid: str) -> bytes:
    resp = get(Endpoints.graph_version_download(graph_version_uid))
    resp.raise_for_status()
    return resp.content


def download_component_zip(component_key: str) -> bytes:
    org, comp, v = COMPONENT_RE.fullmatch(component_key).groups()
    resp = get(Endpoints.component_download(org, comp, v))
    resp.raise_for_status()
    return resp.content
