from __future__ import annotations

from typing import List

from basis.cli.services.api import Endpoints, get, post


def list_objects(obj_type: str, organization_name: str) -> List[dict]:
    if obj_type == "env":
        obj_type = "environment"
    endpoint = getattr(Endpoints, f"{obj_type.upper()}S_LIST")
    resp = get(endpoint, params={"organization_name": organization_name})
    resp.raise_for_status()
    return resp.json().get("results", [])


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
