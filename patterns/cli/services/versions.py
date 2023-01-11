from __future__ import annotations

import requests
from requests import JSONDecodeError
import patterns


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
    if latest == patterns.__version__:
        return None
    return latest
