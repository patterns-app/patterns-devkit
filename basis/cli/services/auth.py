from __future__ import annotations
from basis.cli.config import remove_auth_from_basis_config, update_local_basis_config
from basis.cli.services.api import Endpoints, get, post


def login(email: str, password: str):
    resp = post(Endpoints.TOKEN_CREATE, data={"email": email, "password": password,},)
    resp.raise_for_status()
    data = resp.json()
    update_basis_config_with_auth(data, email=email)


def refresh_token(refresh_token: str):
    print("refreshing token")
    print(refresh_token)
    resp = post(Endpoints.TOKEN_REFRESH, data={"refresh": refresh_token})
    resp.raise_for_status()
    data = resp.json()
    update_basis_config_with_auth(data)


def update_basis_config_with_auth(auth_data: dict, **kwargs):
    update_local_basis_config(
        token=auth_data["access"], refresh=auth_data["refresh"], **kwargs
    )


def logout():
    remove_auth_from_basis_config()


def list_organizations() -> list[dict]:
    # TODO
    resp = get(Endpoints.ORGANIZATIONS_LIST,)
    resp.raise_for_status()
    organizations = resp.json()
    return organizations
