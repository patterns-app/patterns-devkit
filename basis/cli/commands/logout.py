from __future__ import annotations

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.services.auth import logout


class LogoutCommand(BasisCommandBase, Command):
    """
    Logout of getbasis.com account

    logout
    """

    def handle(self):
        logout()
        self.line("<info>Logged out successfully</info>")
