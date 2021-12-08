from __future__ import annotations

from typing import List

from basis.cli.services.api import Endpoints, get


def list_graphs() -> List[dict]:
    resp = get(Endpoints.GRAPHS_LIST)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def list_organizations() -> List[dict]:
    resp = get(Endpoints.ORGANIZATIONS_LIST)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def list_environments() -> List[dict]:
    resp = get(Endpoints.ENVIRONMENTS_LIST)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])
