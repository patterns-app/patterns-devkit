from __future__ import annotations

from basis.cli.services.api import Endpoints, get, post


def trigger_node(
    absolute_node_path: str, graph_version_uid: str, environment_name: str
) -> list[dict]:
    resp = post(
        Endpoints.DEPLOYMENTS_TRIGGER_NODE,
        json={
            "absolute_node_path": absolute_node_path,
            "environment_name": environment_name,
            "graph_version_uid": graph_version_uid,
        },
    )
    resp.raise_for_status()
    return resp.json()
