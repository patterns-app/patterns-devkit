from __future__ import annotations

from requests import Session

from patterns.cli.services.api import Endpoints, post_for_json, patch


def create_graph_component(graph_version_uid: str, session: Session = None) -> dict:
    body = {"graph_version_uid": graph_version_uid}
    return post_for_json(Endpoints.COMPONENTS_CREATE, json=body, session=session)


def update_graph_component(graph_uid: str, deprecated: bool, session: Session = None):
    body = {"deprecated": deprecated}
    return patch(Endpoints.component_update(graph_uid), json=body, session=session)
