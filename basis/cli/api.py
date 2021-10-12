from typing import Dict
from basis.cli.config import read_local_basis_config
import requests
from requests import Session, Request, Response


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


def upload(
    data: Dict, session: Session = None, path: str = "project-version/upload"
) -> Response:
    return get(path, data, session)


def project_info(
    params: Dict, session: Session = None, path: str = "project-version/info"
) -> Response:
    return get(path, params, session)


def app_info(params: Dict, session: Session = None, path: str = "app/info") -> Response:
    return get(path, params, session)


def node_info(
    params: Dict, session: Session = None, path: str = "node/info"
) -> Response:
    return get(path, params, session)


def project_logs(
    params: Dict, session: Session = None, path: str = "project-version/logs"
) -> Response:
    return get(path, params, session)


def app_logs(params: Dict, session: Session = None, path: str = "app/logs") -> Response:
    return get(path, params, session)


def node_logs(
    params: Dict, session: Session = None, path: str = "node/logs"
) -> Response:
    return get(path, params, session)
