import base64
from basis.cli.api import upload
from basis.configuration.base import load_yaml
from basis.configuration.dataspace import DataspaceCfg
import os
from basis.cli.helpers import compress_directory

from cleo import Command

from basis.cli.commands.base import BasisCommandBase


class UploadCommand(BasisCommandBase, Command):
    """
    Upload current dataspace to getbasis.com (as new dataspace version)

    upload
        {dataspace : Path to Basis dataspace config (yaml)}
    """

    def handle(self):
        self.ensure_authenticated()
        cfg = self.argument("dataspace")
        ds_cfg = load_yaml(cfg)
        DataspaceCfg(**ds_cfg)  # Validate yaml
        zipf = compress_directory(os.curdir)
        b64_zipf = base64.b64encode(zipf.read())
        resp = upload({"dataspace_config": ds_cfg, "zip": b64_zipf.decode()})
        if not resp.ok:
            self.line(f"<error>Upload failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(
            f"<info>Uploaded dataspace successfully (Version {data['dataspace_version_id']}</info>"
        )

