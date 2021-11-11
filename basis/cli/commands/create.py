from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import get_current_organization_name
from basis.cli.services.create import create_environment


class CreateCommand(BasisCommandBase, Command):
    """
    Create a new environment on getbasis.com

    create
        {type : Only `env` supported for now}
        {--name= : Environment name}
    """

    def handle(self):
        self.ensure_authenticated()
        type = self.argument("type")
        assert type == "env"
        env_name = self.option("name")
        if not env_name:
            env_name = self.ask(f"Environment name:")
        assert isinstance(env_name, str)
        org_name = get_current_organization_name()
        try:
            env = create_environment(env_name, organization_name=org_name)
            print(env)
        except Exception as e:
            self.line(f"<error>Failed to create env: {e}</error>")
            exit(1)
        self.line(f"Env created (<info>{env['name']}</info>)")
