from __future__ import annotations
import os
from enum import Enum

import requests
from basis.cli.config import read_local_basis_config
from requests import Request, Response, Session

API_BASE_URL = os.environ.get("BASIS_API_URL", "https://api.getbasis.com/")
AUTH_TOKEN_PREFIX = "JWT"


def get_api_session() -> Session:
    s = requests.Session()
    auth_token = get_auth_token()
    if auth_token:
        s.headers.update({"Authorization": f"{AUTH_TOKEN_PREFIX} {auth_token}"})
    return s


def get_auth_token():
    cfg = read_local_basis_config()
    auth_token = cfg.get("token")
    if auth_token:
        resp = requests.post(
            API_BASE_URL + Endpoints.TOKEN_VERIFY, data={"token": auth_token}
        )
        if resp.status_code == 401:
            refresh = cfg.get("refresh")
            if refresh:
                from basis.cli.services.auth import refresh_token

                refresh_token(refresh)
                cfg = read_local_basis_config()
                auth_token = cfg.get("token")
        else:
            resp.raise_for_status()
    return auth_token


def get(path: str, params: dict = None, session: Session = None, **kwargs) -> Response:
    session = session or get_api_session()
    resp = session.get(API_BASE_URL + path, params=params or {}, **kwargs)
    return resp


def post(path: str, data: dict = None, session: Session = None, **kwargs) -> Response:
    session = session or get_api_session()
    resp = session.post(API_BASE_URL + path, json=data or {}, **kwargs)
    return resp


class Endpoints(str, Enum):
    TOKEN_CREATE = "auth/jwt/create/"
    TOKEN_VERIFY = "auth/jwt/verify/"
    TOKEN_REFRESH = "auth/jwt/refresh/"
    GRAPH_VERSIONS_UPLOAD = "api/graph-versions/upload/"
    GRAPH_VERSIONS_DOWNLOAD = "api/graph-versions/download/"
    ENVIRONMENTS_INFO = "api/environments/info/"
    GRAPHS_INFO = "api/graphs/info/"
    NODES_INFO = "api/nodes/info/"
    ENVIRONMENTS_LOGS = "api/environments/logs/"
    GRAPHS_LOGS = "api/graphs/logs/"
    NODES_LOGS = "api/nodes/logs/"
    ORGANIZATIONS_LIST = "api/organizations/"
    ENVIRONMENTS_LIST = "api/environments/"
    GRAPHS_LIST = "api/graphs/"
    NODES_LIST = "api/nodes/"
    NODES_RUN = "api/nodes/"
