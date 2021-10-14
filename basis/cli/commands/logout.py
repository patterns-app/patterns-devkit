from __future__ import annotations

import os
import sys

import requests
from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import remove_auth_from_basis_config, update_local_basis_config
from basis.cli.templates.generator import generate_template
from cleo import Command


class LogoutCommand(BasisCommandBase, Command):
    """
    Logout of getbasis.com account

    logout
    """

    def handle(self):
        remove_auth_from_basis_config()
        self.line("<info>Logged out successfully</info>")
