import base64
from basis.cli.api import upload
from basis.configuration.base import load_yaml
from basis.configuration.project import ProjectCfg
import os
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import List, Pattern
from basis.cli.helpers import compress_directory

from cleo import Command
import requests

from basis.cli.commands.base import BasisCommandBase
from basis.cli.templates.generator import generate_template


class UploadCommand(BasisCommandBase, Command):
    """
    Upload current project to getbasis.com (as new project version)

    upload
        {project : Path to Basis project config (yaml)}
    """

    def handle(self):
        cfg = self.argument("project")
        project_cfg = load_yaml(cfg)
        ProjectCfg(**project_cfg)  # Validate yaml
        zipf = compress_directory(os.curdir)
        b64_zipf = base64.b64encode(zipf.read())
        resp = upload({"project_config": project_cfg, "zip": b64_zipf.decode()})
        if not resp.ok:
            self.line(f"<error>Upload failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(
            f"<info>Uploaded project successfully (Version {data['project_version_id']}</info>"
        )

