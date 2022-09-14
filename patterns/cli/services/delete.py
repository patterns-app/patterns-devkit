from __future__ import annotations

from requests import Session

from patterns.cli.services.api import Endpoints, delete


def delete_graph(graph_uid: str, session: Session = None):
    delete(Endpoints.graph_delete(graph_uid), session=session)
