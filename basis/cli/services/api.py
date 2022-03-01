import os
from typing import Any

import requests
from requests import Response, Session

from basis.cli.config import (
    read_local_basis_config,
    CliConfig,
    write_local_basis_config,
)
from basis.cli.services.output import abort, abort_on_error

API_BASE_URL = (
    os.environ.get("BASIS_API_URL", "https://api-production.getbasis.com/").rstrip("/")
    + "/"
)
AUTH_TOKEN_ENV_VAR = "BASIS_AUTH_TOKEN"
AUTH_TOKEN_PREFIX = "JWT"

PUBLIC_API_BASE_URL = "api/v0"


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
    with abort_on_error(
        "Not logged in", suffix="\n[info]You can log in with [code]basis login"
    ):
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


def get_json(
    path: str,
    params: dict = None,
    session: Session = None,
    base_url: str = API_BASE_URL,
    **kwargs,
) -> Any:
    resp = get(path, params, session, base_url, **kwargs)
    resp.raise_for_status()
    return resp.json()


def get(
    path: str,
    params: dict = None,
    session: Session = None,
    base_url: str = API_BASE_URL,
    **kwargs,
) -> Response:
    session = session or _get_api_session()
    resp = session.get(base_url + path, params=params or {}, **kwargs)
    return resp


def post_for_json(
    path: str,
    json: dict = None,
    session: Session = None,
    base_url: str = API_BASE_URL,
    **kwargs,
) -> Any:
    resp = post(path, json, session, base_url, **kwargs)
    resp.raise_for_status()
    return resp.json()


def post(
    path: str,
    json: dict = None,
    session: Session = None,
    base_url: str = API_BASE_URL,
    **kwargs,
) -> Response:
    session = session or _get_api_session()
    resp = session.post(base_url + path, json=json or {}, **kwargs)
    return resp


class Endpoints:
    TOKEN_CREATE = "auth/jwt/create/"
    TOKEN_VERIFY = "auth/jwt/verify/"
    TOKEN_REFRESH = "auth/jwt/refresh/"
    DEPLOYMENTS_DEPLOY = f"{PUBLIC_API_BASE_URL}/deployments/"
    DEPLOYMENTS_TRIGGER_NODE = f"{PUBLIC_API_BASE_URL}/deployments/triggers/"
    ENVIRONMENTS_CREATE = f"{PUBLIC_API_BASE_URL}/environments/"
    ORGANIZATIONS_LIST = f"{PUBLIC_API_BASE_URL}/organizations/"
    EXECUTION_EVENTS = f"{PUBLIC_API_BASE_URL}/nodes/execution_events/"
    OUTPUT_DATA = f"{PUBLIC_API_BASE_URL}/nodes/output_block_data/latest/"
    WEBHOOKS = f"{PUBLIC_API_BASE_URL}/webhooks/"
    COMPONENTS_LIST = f"{PUBLIC_API_BASE_URL}/marketplace/components/"
    COMPONENTS_CREATE = f"{PUBLIC_API_BASE_URL}/marketplace/components/versions/"

    @classmethod
    def organization_by_name(cls, name: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/organizations/name/{name}/"

    @classmethod
    def organization_by_id(cls, organization_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/organizations/{organization_uid}/"

    @classmethod
    def graphs_list(cls, organization_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/organizations/{organization_uid}/graphs/"

    @classmethod
    def graphs_latest(cls, graph_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/graphs/{graph_uid}/latest/"

    @classmethod
    def graph_version_download(cls, graph_version_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/graph_versions/{graph_version_uid}/zip/"

    @classmethod
    def graph_by_name(cls, organization_uid: str, name: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/organizations/{organization_uid}/graphs/name/{name}/"

    @classmethod
    def graph_by_id(cls, graph_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/graphs/{graph_uid}/"

    @classmethod
    def graph_version_by_id(cls, graph_version_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/graph_versions/{graph_version_uid}/"

    @classmethod
    def graph_version_create(cls, organization_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/organizations/{organization_uid}/graph_versions/"

    @classmethod
    def environments_list(cls, organization_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/organizations/{organization_uid}/environments/"

    @classmethod
    def environment_by_name(cls, organization_uid: str, name: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/organizations/{organization_uid}/environments/name/{name}/"

    @classmethod
    def environment_by_id(cls, environment_uid: str) -> str:
        return f"{PUBLIC_API_BASE_URL}/environments/{environment_uid}/"
