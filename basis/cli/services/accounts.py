from __future__ import annotations

from basis.cli.services.api import Endpoints, get_json


def me() -> dict:
    return get_json(
        Endpoints.ACCOUNTS_ME,
    )
