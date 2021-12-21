from __future__ import annotations

from basis.cli.services.api import Endpoints, get_json


def get_graph_by_name(organization_uid: str, name: str) -> dict:
    return get_json(Endpoints.graph_by_name(organization_uid, name))


def get_active_graph_version(graph_uid: str) -> dict:
    return get_json(Endpoints.graphs_latest(graph_uid))["active_graph_version"]
