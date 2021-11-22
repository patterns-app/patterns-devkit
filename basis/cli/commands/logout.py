from __future__ import annotations

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import remove_auth_from_basis_config


class LogoutCommand(BasisCommandBase, Command):
    """
    Logout of getbasis.com account

    logout
    """

    def handle(self):
        remove_auth_from_basis_config()
        self.line("<info>Logged out successfully</info>")
