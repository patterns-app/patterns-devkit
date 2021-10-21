import base64
import os
from pathlib import Path

from basis.cli.services.api import upload
from basis.cli.commands.base import BasisCommandBase
from basis.cli.helpers import compress_directory
from basis.cli.services.upload import upload_graph_version
from basis.configuration.base import load_yaml
from basis.configuration.graph import GraphCfg
from cleo import Command


class UploadCommand(BasisCommandBase, Command):
    """
    Upload a graph to getbasis.com (as new graph version)

    upload
        {graph : Path to Basis graph config (yaml)}
    """

    def handle(self):
        self.ensure_authenticated()
        cfg = self.argument("graph")
        assert isinstance(cfg, str)
        cfg_path = Path(cfg)
        cfg_dir = cfg_path.parent
        graph_cfg = load_yaml(cfg)
        cfg = GraphCfg(**graph_cfg)
        resp = upload_graph_version(cfg, cfg_dir)
        if not resp.ok:
            self.line(f"<error>Upload failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(
            f"<info>Uploaded dataspace successfully (Version {data['graph_version_id']}</info>"
        )
