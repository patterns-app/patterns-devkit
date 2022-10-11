from __future__ import annotations

import re

from requests import Session

from patterns.cli.services.api import Endpoints, get

COMPONENT_RE = re.compile(r"([\w\-]+)/([\w\-]+)@([\w\-.]+)")


def download_graph_zip(graph_version_uid: str, session: Session = None) -> bytes:
    resp = get(Endpoints.graph_version_download(graph_version_uid), session=session)
    resp.raise_for_status()
    return resp.content


def download_component_zip(component_key: str, session: Session = None) -> bytes:
    org, comp, v = COMPONENT_RE.fullmatch(component_key).groups()
    resp = get(Endpoints.component_download(org, comp, v), session=session)
    resp.raise_for_status()
    return resp.content
