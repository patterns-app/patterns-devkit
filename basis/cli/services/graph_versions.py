from __future__ import annotations

from basis.cli.services.api import Endpoints, get, post
from basis.cli.services.output import abort


def list_graph_versions(graph_name: str, organization_name: str) -> list[dict]:
    resp = get(
        Endpoints.GRAPH_VERSIONS_LIST,
        params={"organization_name": organization_name, "graph_name": graph_name},
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def get_latest_graph_version(graph_name: str, organization_name: str) -> dict:
    versions = list_graph_versions(graph_name, organization_name)
    if not versions:
        abort("No graph versions exist")
    return versions[0]
