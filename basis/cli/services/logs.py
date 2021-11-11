from __future__ import annotations

from basis.cli.services.api import Endpoints, get, post


def get_execution_logs(
    organization_name: str,
    environment_name: str,
    graph_name: str,
    node_path: str | None
) -> list[dict]:
    params = {
        'organization_name': organization_name,
        'environment_name': environment_name,
        'graph_name': graph_name,
    }
    if node_path:
        params['node_path'] = node_path
    resp = get(Endpoints.EXECUTION_EVENTS, params=params)
    resp.raise_for_status()
    return resp.json()['execution_events']
