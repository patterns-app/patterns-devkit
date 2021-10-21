import base64
import os
from io import BytesIO

from basis.cli.commands.base import BasisCommandBase
from basis.cli.helpers import expand_directory
from cleo import Command


class CloneCommand(BasisCommandBase, Command):
    """
    Clone existing graph version into current directory

    clone
        {name : Graph name}
        {--path : Path to expand to}
        {--graph-version : Specific version (defaults to latest version)}
    """

    def handle(self):
        self.ensure_authenticated()
        ds_name = self.argument("name")
        ds_path = self.option("path") or "."
        resp = download({"name": ds_name})
        if not resp.ok:
            self.line(f"<error>Download failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        b64_zipf = data["zip"]
        zip_bytes = base64.b64decode(b64_zipf)
        expand_directory(BytesIO(zip_bytes), ds_path)
        self.line(f"<info>Cloned graph files {ds_name} into {ds_path}</info>")
