from __future__ import annotations

from basis.cli.commands.generate import GenerateCommand
from basis.cli.commands.output import OutputCommand
from basis.cli.commands.run import RunCommand
from cleo.application import Application

app = Application("basis")
app.add(GenerateCommand())
app.add(RunCommand())
app.add(OutputCommand())


if __name__ == "__main__":
    app.run()
