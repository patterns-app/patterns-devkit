from __future__ import annotations

import requests
from requests import JSONDecodeError
import patterns
from patterns.cli.services.output import sprint

"""Set to true to disable version checking"""
DISABLE_VERSION_CHECK = False

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

def print_message_if_devkit_needs_update():
    if DISABLE_VERSION_CHECK:
        return
    latest = get_newer_devkit_version()
    if not latest:
        return

    sprint(
        f"\n[info]A newer version of the Patterns devkit "
        f"([error]{patterns.__version__}[/error] -> [success]{latest}[/success]) is available."
    )
    sprint(
        "[info]Run [code]pip install --upgrade patterns-devkit[/code] "
        "to get the latest version."
    )
