from __future__ import annotations
from basis.cli.commands.auth import LoginCommand, LogoutCommand

from basis.cli.commands.generate import GenerateCommand
from basis.cli.commands.upload import UploadCommand
from cleo.application import Application

app = Application("basis")
app.add(GenerateCommand())
app.add(LoginCommand())
app.add(LogoutCommand())
app.add(UploadCommand())


if __name__ == "__main__":
    app.run()
