from pathlib import Path

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.services.upload import upload_graph_version
from basis.cli.config import read_local_basis_config


class UploadCommand(BasisCommandBase, Command):
    """
    Upload a graph to getbasis.com (as new graph version)

    upload
        {graph : Path to Basis graph config (yaml)}
    """

    def handle(self):
        self.ensure_authenticated()
        cfg_arg = self.argument("graph")
        assert isinstance(cfg_arg, str)
        cfg_path = Path(cfg_arg)
        try:
            cfg = read_local_basis_config()
            data = upload_graph_version(cfg_path, cfg.organization_name)
        except Exception as e:
            self.line(f"<error>Upload failed: {e}</error>")
            if hasattr(e, "response"):
                self.line(f"\t<error>{e.response.json()}</error>")
            exit(1)
        self.line(f"Graph uploaded successfully (Version <info>{data['uid']}</info>)")
