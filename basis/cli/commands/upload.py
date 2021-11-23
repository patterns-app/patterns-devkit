from pathlib import Path

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import get_current_organization_name
from basis.cli.services.upload import upload_graph_version


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
            data = upload_graph_version(cfg_path, get_current_organization_name())
        except Exception as e:
            self.line(f"<error>Upload failed: {e}</error>")
            exit(1)
        self.line(f"Graph uploaded successfully (Version <info>{data['uid']}</info>)")
