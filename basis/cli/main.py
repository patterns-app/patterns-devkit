from typing import List, Optional

import typer
from click import Group, Context, HelpFormatter, MultiCommand, Command

from .commands.config import config
from .commands.create import create
from .commands.deploy import deploy
from .commands.list import list_command
from .commands.login import login
from .commands.logout import logout
from .commands.manifest import manifest
from .commands.pull import pull, clone
from .commands.trigger import trigger
from .commands.upload import upload


class _Command(Group):
    def __init__(self):
        super().__init__(name="basis", no_args_is_help=True)

    def add_typer_fn(self, fn, **kw):
        if isinstance(command, typer.Typer):
            self.add_command(typer.main.get_command(fn))
        else:
            tmp = typer.Typer()
            tmp.command(**kw)(command)
            self.add_command(typer.main.get_command(tmp))

    # override help output to include nested subcommands
    def format_commands(self, ctx: Context, formatter: HelpFormatter) -> None:
        old_list = self.list_commands
        old_get = self.get_command

        self.list_commands = self._list_commands
        self.get_command = self._get_command

        super().format_commands(ctx, formatter)

        self.list_commands = old_list
        self.get_command = old_get

    def _list_commands(self, ctx: Context) -> List[str]:
        l = super().list_commands(ctx)
        for c in l:
            sub = super().get_command(ctx, c)
            if isinstance(sub, MultiCommand):
                l.extend(f"{c} {s}" for s in sub.list_commands(ctx))
        return l

    def _get_command(self, ctx: Context, cmd_name: str) -> Optional[Command]:
        parts = cmd_name.split()
        base = super().get_command(ctx, parts[0])
        if len(parts) == 1:
            return base
        assert len(parts) == 2
        assert isinstance(base, MultiCommand)
        return base.get_command(ctx, parts[1])


app = _Command()

for command in (
    config,
    create,
    deploy,
    list_command,
    login,
    logout,
    trigger,
    upload,
    pull,
    clone,
):
    app.add_typer_fn(command)

# don't show manifest command in help
app.add_typer_fn(manifest, hidden=True)


def main():
    app()
