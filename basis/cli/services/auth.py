from __future__ import annotations

from basis.cli.config import (
    remove_auth_from_basis_config,
    update_basis_config_with_auth,
)
from basis.cli.services.api import Endpoints, get, post


def login(email: str, password: str):
    resp = post(Endpoints.TOKEN_CREATE, data={"email": email, "password": password,},)
    resp.raise_for_status()
    data = resp.json()
    update_basis_config_with_auth(data, email=email)


def logout():
    remove_auth_from_basis_config()


def list_organizations() -> list[dict]:
    # TODO
    resp = get(Endpoints.ORGANIZATIONS_LIST,)
    resp.raise_for_status()
    organizations = resp.json()
    return organizations.get("results", [])
