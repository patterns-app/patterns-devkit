from __future__ import annotations

from basis.cli.services.api import Endpoints, get, post


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
        raise ValueError("No graph versions exist")
    latest_version = versions[-1]
    # TODO
    # for v in versions:
    #     if v["created_at"] > latest_version["created_at"]:
    #         latest_version = v
    return latest_version
