import typer

from .newcommands.config import config
from .newcommands.create import create
from .newcommands.deploy import deploy
from .newcommands.list import list_command
from .newcommands.login import login
from .newcommands.logout import logout
from .newcommands.trigger import trigger
from .newcommands.upload import upload

app = typer.Typer(add_completion=False)

for command in (
    config, create, deploy, list_command, login, logout, trigger, upload
):
    if isinstance(command, typer.Typer):
        app.add_typer(command)
    else:
        app.command()(command)


def main():
    app()
