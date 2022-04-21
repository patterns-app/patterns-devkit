from __future__ import annotations

from basis.cli.services.api import Endpoints, get_json


def get_graph_by_slug(organization_uid: str, slug: str) -> dict:
    return get_json(Endpoints.graph_by_slug(organization_uid, slug))


def get_graph_version_by_id(graph_version_uid) -> dict:
    return get_json(Endpoints.graph_version_by_id(graph_version_uid))


def get_active_graph_version(graph_uid: str) -> dict:
    return get_json(Endpoints.graphs_latest(graph_uid))["active_graph_version"]
