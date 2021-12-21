from __future__ import annotations

from basis.cli.services.api import Endpoints, post_for_json


def deploy_graph_version(graph_version_uid: str, environment_uid: str) -> dict:
    return post_for_json(
        Endpoints.DEPLOYMENTS_DEPLOY,
        json={
            "environment_uid": environment_uid,
            "graph_version_uid": graph_version_uid,
        },
    )
