from __future__ import annotations

from basis.cli.services.api import Endpoints, get, post


def deploy_graph_version(graph_version_uid: str, environment_name: str) -> list[dict]:
    resp = post(
        Endpoints.DEPLOYMENTS_DEPLOY,
        json={
            "environment_name": environment_name,
            "graph_version_uid": graph_version_uid,
        },
    )
    resp.raise_for_status()
    return resp.json()
