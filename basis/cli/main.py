import typer

from .commands.config import config
from .commands.create import create
from .commands.deploy import deploy
from .commands.list import list_command
from .commands.login import login
from .commands.logout import logout
from .commands.manifest import manifest
from .commands.trigger import trigger
from .commands.upload import upload

app = typer.Typer(add_completion=False)

for command in (
    config,
    create,
    deploy,
    list_command,
    login,
    logout,
    trigger,
    upload,
):
    if isinstance(command, typer.Typer):
        app.add_typer(command)
    else:
        app.command()(command)

# don't show manifest command in help
app.command(hidden=True)(manifest)


def main():
    app()
