import base64
import os

from basis.cli.api import upload
from basis.cli.commands.base import BasisCommandBase
from basis.cli.helpers import compress_directory
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
        ds_cfg = load_yaml(cfg)
        GraphCfg(**ds_cfg)  # Validate yaml
        zipf = compress_directory(os.curdir)
        b64_zipf = base64.b64encode(zipf.read())
        resp = upload({"graph_config": ds_cfg, "zip": b64_zipf.decode()})
        if not resp.ok:
            self.line(f"<error>Upload failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(
            f"<info>Uploaded dataspace successfully (Version {data['graph_version_id']}</info>"
        )
