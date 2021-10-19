from typing import Dict

import requests
from basis.cli.config import read_local_basis_config
from requests import Request, Response, Session

DEFAULT_BASE_URL = "https://api.getbasis.com/"
AUTH_TOKEN_PREFIX = "JWT"


def get_api_session() -> Session:
    s = requests.Session()
    cfg = read_local_basis_config()
    if "token" in cfg:
        s.headers.update({"Authorization": f"{AUTH_TOKEN_PREFIX} {cfg['token']}"})
    return s


def get(path: str, data: Dict, session: Session = None) -> Response:
    session = session or get_api_session()
    resp = session.post(DEFAULT_BASE_URL + path, json=data)
    return resp


def post(path: str, data: Dict, session: Session = None) -> Response:
    session = session or get_api_session()
    resp = session.post(DEFAULT_BASE_URL + path, json=data)
    return resp


def login(
    data: Dict, session: Session = None, path: str = "api/token-auth"
) -> Response:
    return post(path, data, session)


def run_node(data: Dict, session: Session = None, path: str = "nodes/run") -> Response:
    return post(path, data, session)


def upload(
    data: Dict, session: Session = None, path: str = "graph-versions/upload"
) -> Response:
    return post(path, data, session)


def download(
    params: Dict, session: Session = None, path: str = "graph-versions/download"
) -> Response:
    return get(path, params, session)


def env_info(
    params: Dict, session: Session = None, path: str = "environments/info"
) -> Response:
    return get(path, params, session)


def graph_info(
    params: Dict, session: Session = None, path: str = "graphs/info"
) -> Response:
    return get(path, params, session)


def node_info(
    params: Dict, session: Session = None, path: str = "nodes/info"
) -> Response:
    return get(path, params, session)


def env_logs(
    params: Dict, session: Session = None, path: str = "environments/logs"
) -> Response:
    return get(path, params, session)


def graph_logs(
    params: Dict, session: Session = None, path: str = "graphs/logs"
) -> Response:
    return get(path, params, session)


def node_logs(
    params: Dict, session: Session = None, path: str = "nodes/logs"
) -> Response:
    return get(path, params, session)


def env_list(
    params: Dict = None, session: Session = None, path: str = "environments"
) -> Response:
    return get(path, params or {}, session)


def graph_list(
    params: Dict = None, session: Session = None, path: str = "graphs"
) -> Response:
    return get(path, params or {}, session)


def node_list(
    params: Dict = None, session: Session = None, path: str = "nodes"
) -> Response:
    return get(path, params or {}, session)
