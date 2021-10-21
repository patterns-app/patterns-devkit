import base64
import os
from pathlib import Path
from basis.cli.config import get_current_organization_uid

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
        cfg_arg = self.argument("graph")
        assert isinstance(cfg_arg, str)
        graph_cfg = graph_cfg_from_argument(cfg_arg)
        cfg_path = Path(cfg_arg)
        cfg_dir = cfg_path.parent
        try:
            data = upload_graph_version(
                graph_cfg, cfg_dir, get_current_organization_uid()
            )
        except Exception as e:
            self.line(f"<error>Upload failed: {e}</error>")
            exit(1)
        self.line(f"Graph uploaded successfully (Version <info>{data['uid']}</info>)")


def graph_cfg_from_argument(cfg_arg: str) -> GraphCfg:
    graph_cfg = load_yaml(cfg_arg)
    cfg = GraphCfg(**graph_cfg)
    return cfg
