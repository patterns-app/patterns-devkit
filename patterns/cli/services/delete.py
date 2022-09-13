from __future__ import annotations

from patterns.cli.services.api import Endpoints, delete


def delete_graph(graph_uid: str):
    delete(Endpoints.graph_delete(graph_uid))
