from __future__ import annotations

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.services.download import download_graph_version
from basis.cli.config import read_local_basis_config


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
        graph_name = self.argument("name")
        expansion_path = self.option("path") or "."
        assert isinstance(graph_name, str)
        try:
            cfg = read_local_basis_config()
            data = download_graph_version(graph_name, cfg.organization_name)
        except Exception as e:
            self.line(f"<error>Clone failed: {e}</error>")
            exit(1)
        # TODO
        # b64_zipf = data["zip"]
        # zip_bytes = base64.b64decode(b64_zipf)
        # expand_directory(BytesIO(zip_bytes), ds_path)
        # self.line(f"<info>Cloned graph files {ds_name} into {ds_path}</info>")
