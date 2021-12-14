from __future__ import annotations

from enum import Enum

from basis.cli.services.api import Endpoints, post


class TypeChoices(str, Enum):
    PUBSUB = "PUBSUB"
    HTTP = "HTTP"
    LOCAL = "LOCAL"


def trigger_node(
    node_id: str,
    graph_version_uid: str,
    environment_name: str,
    execution_type: TypeChoices = TypeChoices.PUBSUB,
) -> list[dict]:
    resp = post(
        Endpoints.DEPLOYMENTS_TRIGGER_NODE,
        json={
            "node_id": node_id,
            "environment_name": environment_name,
            "graph_version_uid": graph_version_uid,
            "execution_type": execution_type.name,
        },
    )
    resp.raise_for_status()
    return resp.json()
