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


def graph_components_all() -> Iterable[dict]:
    """Iterate over all available graph components"""
    yield from paginated_graph_components_admin()
    yield from paginated_graph_components_regular()


@paginated
def paginated_graph_components_user():
    """Components from this user's organization"""
    return get_json(Endpoints.COMPONENTS_USER)


@paginated
def paginated_graph_components_admin():
    """Components from the system"""
    return get_json(Endpoints.COMPONENTS_ADMIN)


@paginated
def paginated_graph_components_regular():
    """Components not from the system (a superset of user components)"""
    return get_json(Endpoints.COMPONENTS_REGULAR)
