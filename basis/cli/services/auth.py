import base64
import os
from pathlib import Path
from typing import Dict, List

from requests.models import Response
from basis.cli.config import remove_auth_from_basis_config, update_local_basis_config

from basis.cli.helpers import compress_directory
from basis.cli.services.api import Endpoints, post, get
from basis.configuration.graph import GraphCfg

from basis.graph.builder import ConfiguredGraphBuilder


def login(email: str, password: str):
    resp = post(Endpoints.TOKEN_AUTH, data={"email": email, "password": password,})
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


def update_basis_config_with_auth(auth_data: Dict, **kwargs):
    update_local_basis_config(
        token=auth_data["access"], refresh=auth_data["refresh"], **kwargs
    )


def logout():
    remove_auth_from_basis_config()


def list_organizations() -> List[Dict]:
    # TODO
    resp = get(Endpoints.ORGANIZATIONS_LIST,)
    resp.raise_for_status()
    organizations = resp.json()
    return organizations

