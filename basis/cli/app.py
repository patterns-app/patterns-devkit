from __future__ import annotations

from basis.cli.commands.generate import GenerateCommand
from cleo.application import Application

app = Application("basis")
app.add(GenerateCommand())


if __name__ == "__main__":
    app.run()
