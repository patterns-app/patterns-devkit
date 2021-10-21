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
    update_local_basis_config(
        token=data["access"], refresh=data["refresh"], email=email
    )


def logout():
    remove_auth_from_basis_config()


def list_organizations() -> List[Dict]:
    # TODO
    resp = get(Endpoints.ORGANIZATIONS_LIST,)
    resp.raise_for_status()
    organizations = resp.json()
    return organizations

