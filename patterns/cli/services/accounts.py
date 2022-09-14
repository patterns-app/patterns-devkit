from __future__ import annotations

from requests import Session

from patterns.cli.services.api import Endpoints, get_json


def me(session: Session = None) -> dict:
    return get_json(Endpoints.ACCOUNTS_ME, session=session)
