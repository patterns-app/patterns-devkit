from __future__ import annotations

import os
import sys
from cleo import Command
import requests

from basis.cli.config import (
    remove_auth_from_basis_config,
    update_local_basis_config,
)
from basis.cli.commands.base import BasisCommandBase
from basis.cli.templates.generator import generate_template


DEFAULT_LOGIN_ENDPOINT = "https://api.getbasis.com/api/token-auth/"


class LoginCommand(BasisCommandBase, Command):
    """
    Login to getbasis.com account

    login
        {--e|email=  : getbasis.com email}
        {--p|password= : getbasis.com password}
        {--endpoint= : Login endpoint }
        {--c|config= : Path to basis config }
    """

    def handle(self):
        em = self.option("email")
        pw = self.option("password")
        if not em:
            em = self.ask("Email:")
        if not pw:
            pw = self.secret("Password:")
        endpoint = self.option("endpoint") or DEFAULT_LOGIN_ENDPOINT
        session = self.get_api_session()
        resp = session.post(endpoint, json={"email": em, "password": pw})
        if not resp.ok:
            self.line(f"<error>Login failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        cfg_pth = self.option("config")
        update_local_basis_config(pth=cfg_pth, **data)
        self.line("<info>Logged in successfully</info>")


class LogoutCommand(BasisCommandBase, Command):
    """
    Logout of getbasis.com account

    logout
    """

    def handle(self):
        remove_auth_from_basis_config()
        self.line("<info>Logged out successfully</info>")

