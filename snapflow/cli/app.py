from __future__ import annotations

from cleo.application import Application
from snapflow.cli.commands.generate import GenerateCommand

app = Application("snapflow")
app.add(GenerateCommand())
app.run()
