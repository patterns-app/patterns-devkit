from __future__ import annotations
from basis.cli.commands.auth import LoginCommand, LogoutCommand

from basis.cli.commands.generate import GenerateCommand
from cleo.application import Application

app = Application("basis")
app.add(GenerateCommand())
app.add(LoginCommand())
app.add(LogoutCommand())


if __name__ == "__main__":
    app.run()
