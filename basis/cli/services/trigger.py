from __future__ import annotations

from basis.cli.services.api import Endpoints, post


def trigger_node(
    node_id: str,
    graph_version_uid: str,
    environment_name: str,
    local_execution: bool = False,
) -> list[dict]:
    resp = post(
        Endpoints.DEPLOYMENTS_TRIGGER_NODE,
        json={
            "node_id": node_id,
            "environment_name": environment_name,
            "graph_version_uid": graph_version_uid,
            "local_execution": local_execution,
        },
    )
    resp.raise_for_status()
    return resp.json()
