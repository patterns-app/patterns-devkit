from typing import Dict

import os
from enum import Enum
import requests
from basis.cli.config import read_local_basis_config
from requests import Request, Response, Session

API_BASE_URL = os.environ.get("BASIS_API_URL", "https://api.getbasis.com/")
AUTH_TOKEN_PREFIX = "JWT"


def get_api_session() -> Session:
    s = requests.Session()
    cfg = read_local_basis_config()
    if cfg.get("token"):
        s.headers.update({"Authorization": f"{AUTH_TOKEN_PREFIX} {cfg['token']}"})
    return s


def get(path: str, params: Dict = None, session: Session = None, **kwargs) -> Response:
    session = session or get_api_session()
    resp = session.get(API_BASE_URL + path, params=params or {}, **kwargs)
    return resp


def post(path: str, data: Dict = None, session: Session = None, **kwargs) -> Response:
    session = session or get_api_session()
    resp = session.post(API_BASE_URL + path, json=data or {}, **kwargs)
    return resp


class Endpoints(str, Enum):
    TOKEN_AUTH = "auth/jwt/create/"
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
