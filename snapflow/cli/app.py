from __future__ import annotations

from cleo.application import Application
from snapflow.cli.commands.generate import GenerateCommand
from snapflow.cli.commands.output import OutputCommand
from snapflow.cli.commands.run import RunCommand

app = Application("snapflow")
app.add(GenerateCommand())
app.add(RunCommand())
app.add(OutputCommand())


if __name__ == "__main__":
    app.run()
