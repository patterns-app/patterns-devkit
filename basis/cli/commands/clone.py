import base64
from io import BytesIO
from basis.cli.api import download
import os
from basis.cli.helpers import expand_directory

from cleo import Command

from basis.cli.commands.base import BasisCommandBase


class CloneCommand(BasisCommandBase, Command):
    """
    Clone existing project into current directory

    clone
        {name : Project name}
        {--path : Path to expand to}
    """

    def handle(self):
        self.ensure_authenticated()
        proj_name = self.argument("name")
        proj_path = self.option("path") or "."
        resp = download({"name": proj_name})
        if not resp.ok:
            self.line(f"<error>Download failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        b64_zipf = data["zip"]
        zip_bytes = base64.b64decode(b64_zipf)
        expand_directory(BytesIO(zip_bytes), proj_path)
        self.line(f"<info>Cloned project {proj_name} into {proj_path}</info>")

