from __future__ import annotations

import requests

from basis.cli.config import update_local_basis_config
from basis.cli.services.api import Endpoints, post_for_json


def login(email: str, password: str):
    data = post_for_json(
        Endpoints.TOKEN_CREATE,
        data={"email": email, "password": password},
        session=requests.Session(),
    )  # explicit session to skip auth check
    update_local_basis_config(
        refresh=data["refresh"], token=data["access"], email=email
    )


def logout():
    update_local_basis_config(token=None, refresh=None)
