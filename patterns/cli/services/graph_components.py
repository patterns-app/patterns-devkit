from __future__ import annotations

from requests import Session

from patterns.cli.services.api import Endpoints, post_for_json


def create_graph_component(graph_version_uid: str, session: Session = None) -> dict:
    body = {"graph_version_uid": graph_version_uid}
    return post_for_json(Endpoints.COMPONENTS_CREATE, json=body, session=session)
