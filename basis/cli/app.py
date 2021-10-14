from __future__ import annotations
from basis.cli.commands.clone import CloneCommand
from basis.cli.commands.info import InfoCommand
from basis.cli.commands.login import LoginCommand
from basis.cli.commands.logout import LogoutCommand

from basis.cli.commands.generate import GenerateCommand
from basis.cli.commands.logs import LogsCommand
from basis.cli.commands.run import RunCommand
from basis.cli.commands.upload import UploadCommand
from cleo.application import Application

app = Application("basis")
app.add(GenerateCommand())
app.add(UploadCommand())
app.add(CloneCommand())
app.add(LoginCommand())
app.add(LogoutCommand())
app.add(InfoCommand())
app.add(LogsCommand())
app.add(RunCommand())


if __name__ == "__main__":
    app.run()
