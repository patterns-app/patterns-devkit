import os
from enum import Enum

import requests
from requests import Response, Session

from basis.cli.config import (
    read_local_basis_config,
    CliConfig,
    write_local_basis_config,
)
from basis.cli.services.output import abort, abort_on_error

API_BASE_URL = os.environ.get("BASIS_API_URL", "https://api.getbasis.com/")
AUTH_TOKEN_ENV_VAR = "BASIS_AUTH_TOKEN"
AUTH_TOKEN_PREFIX = "JWT"


def _get_api_session() -> Session:
    s = requests.Session()
    auth_token = _get_auth_token()
    s.headers.update(
        {
            "Authorization": f"{AUTH_TOKEN_PREFIX} {auth_token}",
            "Accept": "application/json",
        }
    )
    return s


def _get_auth_token() -> str:
    override = os.environ.get(AUTH_TOKEN_ENV_VAR)
    if override:
        return override

    cfg = read_local_basis_config()
    if not cfg.token:
        abort("You must be logged in to use this command. Run 'basis login'.")

    with abort_on_error("Failed verifying auth token"):
        resp = requests.post(
            API_BASE_URL + Endpoints.TOKEN_VERIFY, json={"token": cfg.token}
        )
    if resp.status_code == 401:
        if refresh := cfg.refresh:
            cfg = _refresh_token(refresh)
    else:
        resp.raise_for_status()
    return cfg.token


def _refresh_token(token: str) -> CliConfig:
    with abort_on_error("Failed refreshing auth token"):
        resp = requests.post(
            API_BASE_URL + Endpoints.TOKEN_REFRESH, json={"refresh": token}
        )
    resp.raise_for_status()
    data = resp.json()
    cfg = read_local_basis_config()
    if "refresh" in data:
        cfg.refresh = data["refresh"]
    cfg.token = data["access"]

    write_local_basis_config(cfg)
    return cfg


def get(path: str, params: dict = None, session: Session = None, **kwargs) -> Response:
    session = session or _get_api_session()
    resp = session.get(API_BASE_URL + path, params=params or {}, **kwargs)
    return resp


def post(path: str, json: dict = None, session: Session = None, **kwargs) -> Response:
    session = session or _get_api_session()
    resp = session.post(API_BASE_URL + path, json=json or {}, **kwargs)
    return resp


class Endpoints(str, Enum):
    TOKEN_CREATE = "auth/jwt/create/"
    TOKEN_VERIFY = "auth/jwt/verify/"
    TOKEN_REFRESH = "auth/jwt/refresh/"
    DEPLOYMENTS_DEPLOY = "api/deployments/"
    DEPLOYMENTS_TRIGGER_NODE = "api/deployments/triggers/"
    GRAPH_VERSIONS_CREATE = "api/graph_versions/"
    GRAPH_VERSIONS_LIST = "api/graph_versions/"
    ENVIRONMENTS_CREATE = "api/environments/"
    ENVIRONMENTS_INFO = "api/environments/info/"
    GRAPHS_INFO = "api/graphs/info/"
    NODES_INFO = "api/nodes/info/"
    ENVIRONMENTS_LOGS = "api/environments/logs/"
    GRAPHS_LOGS = "api/graphs/logs/"
    NODES_LOGS = "api/nodes/logs/"
    ORGANIZATIONS_LIST = "api/organizations/"
    ENVIRONMENTS_LIST = "api/environments/"
    GRAPHS_LIST = "api/graphs/"
    NODES_RUN = "api/nodes/"
    EXECUTION_EVENTS = "api/execution_events/"
