from __future__ import annotations

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import set_current_organization_name
from basis.cli.services.auth import list_organizations, login, logout


class LoginCommand(BasisCommandBase, Command):
    """
    Login to getbasis.com account

    login
        {--e|email=  : getbasis.com email}
        {--p|password= : getbasis.com password}
    """

    def handle(self):
        # Logout first
        logout()
        em = self.option("email")
        pw = self.option("password")
        if not em:
            em = self.ask("Email:")
        if not pw:
            pw = self.secret("Password:")
        assert isinstance(em, str)
        assert isinstance(pw, str)
        try:
            login(email=em, password=pw)
        except Exception as e:
            self.line(f"<error>Login failed: {e}</error>")
            exit(1)
        self.line("<info>Logged in successfully</info>")
        try:
            organizations = list_organizations()
        except Exception as e:
            self.line(f"<error>Fetching organizations failed: {e}</error>")
            exit(1)
        if not organizations:
            raise Exception("User has no organizations")
        org_name = ""
        if len(organizations) == 1:
            org = organizations[0]
            org_name = org["name"]
            set_current_organization_name(org_name)
        else:
            org_name = self.choice(
                "Select an organization", [org["name"] for org in organizations]
            )
            set_current_organization_name(org_name)
        self.line(f"Using organization <info>{org_name}</info>")
