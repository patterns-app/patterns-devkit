from __future__ import annotations
from basis.cli.commands.info import InfoCommand
from basis.cli.commands.login import LoginCommand
from basis.cli.commands.logout import LogoutCommand

from basis.cli.commands.create import CreateCommand
from basis.cli.commands.logs import LogsCommand
from basis.cli.commands.upload import UploadCommand
from cleo.application import Application

app = Application("basis")
app.add(CreateCommand())
app.add(LoginCommand())
app.add(LogoutCommand())
app.add(UploadCommand())
app.add(InfoCommand())
app.add(LogsCommand())


if __name__ == "__main__":
    app.run()
