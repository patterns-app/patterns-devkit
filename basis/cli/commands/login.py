from __future__ import annotations

import os
import sys

from basis.cli.api import login
from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import remove_auth_from_basis_config, update_local_basis_config
from basis.cli.templates.generator import generate_template
from cleo import Command


class LoginCommand(BasisCommandBase, Command):
    """
    Login to getbasis.com account

    login
        {--e|email=  : getbasis.com email}
        {--p|password= : getbasis.com password}
    """

    def handle(self):
        em = self.option("email")
        pw = self.option("password")
        if not em:
            em = self.ask("Email:")
        if not pw:
            pw = self.secret("Password:")
        resp = login({"email": em, "password": pw})
        if not resp.ok:
            self.line(f"<error>Login failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        update_local_basis_config(**data)
        self.line("<info>Logged in successfully</info>")
