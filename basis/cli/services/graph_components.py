from __future__ import annotations

from basis.cli.services.api import Endpoints, post_for_json


def create_graph_component(
    name: str,
    file_path: str,
    graph_version_uid: str,
    description: str = "",
    icon_url: str = "",
) -> dict:
    body = {
        "name": name,
        "file_path": file_path,
        "graph_version_uid": graph_version_uid,
    }
    if description:
        body["description"] = description
    if icon_url:
        body["icon_url"] = icon_url
    return post_for_json(Endpoints.COMPONENTS_CREATE, json=body)
