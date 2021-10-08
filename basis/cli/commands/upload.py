from __future__ import annotations

import base64
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


DEFAULT_PROJECT_UPLOAD_ENDPOINT = "https://api.getbasis.com/api/project-version/upload"


class UploadCommand(BasisCommandBase, Command):
    """
    Upload current project to getbasis.com (as new project version)

    upload
        {config: Basis project config yaml}
        {--endpoint= : Upload endpoint }
    """

    def handle(self):
        cfg = self.argument("config")
        zipf = compress_directory(os.curdir)
        b64_zipf = base64.b64encode(zipf.read())
        endpoint = self.option("endpoint") or DEFAULT_PROJECT_UPLOAD_ENDPOINT
        session = self.get_api_session()
        resp = session.post(endpoint, json={"project_config": cfg, "zip": b64_zipf})
        if not resp.ok:
            self.line(f"<error>Upload failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(
            f"<info>Uploaded project successfully (Version {data['product_version_id']}</info>"
        )

