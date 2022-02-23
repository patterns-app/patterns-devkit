from __future__ import annotations

from typing import Iterable

from basis.cli.services.api import Endpoints, get_json
from basis.cli.services.pagination import paginated


@paginated
def paginated_graphs(organization_uid: str):
    return get_json(Endpoints.graphs_list(organization_uid))


@paginated
def paginated_execution_events(environment_uid: str, graph_uid: str, node_id: str):
    return get_json(
        Endpoints.EXECUTION_EVENTS,
        params={
            "environment_uid": environment_uid,
            "graph_uid": graph_uid,
            "node_id": node_id,
        },
    )


@paginated
def paginated_output_data(
    environment_uid: str, graph_uid: str, node_id: str, output_port_name: str
):
    return get_json(
        Endpoints.OUTPUT_DATA,
        params={
            "environment_uid": environment_uid,
            "graph_uid": graph_uid,
            "node_id": node_id,
            "output_port_name": output_port_name,
        },
    )


@paginated
def paginated_webhook_urls(
    environment_uid: str, graph_uid: str,
):
    return get_json(
        Endpoints.WEBHOOKS,
        params={"environment_uid": environment_uid, "graph_uid": graph_uid},
    )


@paginated
def paginated_graph_components():
    """All public components"""
    return get_json(Endpoints.COMPONENTS_LIST)
