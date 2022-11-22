from __future__ import annotations

import requests
from requests import JSONDecodeError

CURRENT_DEVKIT_VERSION = "1.4.0"


def get_newer_devkit_version() -> str | None:
    """Return the version number of the latest devkit version on PyPI, or None if the
    local version is up-to-date.
    """

    response = requests.get("https://pypi.python.org/pypi/patterns-devkit/json")
    if not response.ok:
        return None

    try:
        data = response.json()
    except JSONDecodeError:
        return None

    releases = data.get("releases")
    if not isinstance(releases, dict):
        return None

    latest = max(releases)
    if latest == CURRENT_DEVKIT_VERSION:
        return None
    return latest
