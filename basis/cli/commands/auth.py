from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import List, Pattern

from cleo import Command
import requests

from basis.cli.commands.base import BasisCommandBase
from basis.cli.templates.generator import generate_template


DEFAULT_LOGIN_ENDPOINT = "https://api.getbasis.com/api/token-auth/"
CREDENTIALS_PATH = "~/.basis-config.json"


class LoginCommand(BasisCommandBase, Command):
    """
    Login to getbasis.com account

    login
        {--e|email=  : getbasis.com email}
        {--p|password= : getbasis.com password}
        {--endpoint= : Login endpoint }
    """

    def handle(self):
        em = self.option("email")
        pw = self.option("password")
        if not em:
            em = self.ask("Email:")
        if not pw:
            pw = self.secret("Password:")
        endpoint = self.option("endpoint") or DEFAULT_LOGIN_ENDPOINT
        resp = requests.post(endpoint, json={"email": em, "password": pw})
        if not resp.ok:
            self.line(f"<error>Login failed: {resp.text}</error>")
            exit(1)
        data = resp.text
        with open(Path(CREDENTIALS_PATH).expanduser(), "w") as f:
            f.write(data)
        self.line("<info>Logged in successfully</info>")


class LogoutCommand(BasisCommandBase, Command):
    """
    Logout of getbasis.com account
    """

    def handle(self):
        pth = Path(CREDENTIALS_PATH).expanduser()
        if os.path.exists(pth):
            os.remove(pth)
        self.line("<info>Logged out successfully</info>")

