from __future__ import annotations

from basis.cli.services.api import Endpoints, get, post


def create_environment(environment_name: str, organization_name: str) -> list[dict]:
    resp = post(
        Endpoints.ENVIRONMENTS_CREATE,
        json={
            "name": environment_name,
            "organization_name": organization_name,
        },
    )
    resp.raise_for_status()
    return resp.json()
