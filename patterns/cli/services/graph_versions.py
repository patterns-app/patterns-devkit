from __future__ import annotations

from requests import Session

from patterns.cli.services.api import Endpoints, get_json


def get_graph_by_slug(
    organization_uid: str, slug: str, session: Session = None
) -> dict:
    return get_json(Endpoints.graph_by_slug(organization_uid, slug), session=session)


def get_graph_by_uid(graph_uid: str, session: Session = None) -> dict:
    return get_json(Endpoints.graphs_latest(graph_uid), session=session)


def get_graph_version_by_id(graph_version_uid, session: Session = None) -> dict:
    return get_json(Endpoints.graph_version_by_id(graph_version_uid), session=session)


def get_latest_graph_version(graph_uid: str, session: Session = None) -> dict:
    return get_graph_by_uid(graph_uid, session=session)["active_graph_version"]
