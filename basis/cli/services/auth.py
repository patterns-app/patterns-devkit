from __future__ import annotations

from basis.cli.config import update_local_basis_config
from basis.cli.services.api import Endpoints, post


def login(email: str, password: str):
    resp = post(Endpoints.TOKEN_CREATE, data={"email": email, "password": password})
    resp.raise_for_status()
    data = resp.json()
    update_local_basis_config(
        refresh=data["refresh"], token=data["access"], email=email
    )


def logout():
    update_local_basis_config(token=None, refresh=None)
