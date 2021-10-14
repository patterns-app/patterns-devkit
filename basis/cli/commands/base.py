from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Pattern
from basis.cli.config import read_local_basis_config
from basis.configuration.base import dump_yaml, load_yaml
from basis.configuration.project import ProjectCfg

from basis.cli.templates.generator import generate_template, insert_into_file
from cleo import Command
import requests


class BasisCommandBase:
    def is_authenticated(self):
        cfg = read_local_basis_config()
        return bool(cfg.get("token"))

    def ensure_authenticated(self):
        if not self.is_authenticated():
            self.line(
                f"<error>You must login before running this command (`basis login`)</error>"
            )
            exit(1)

    # def get_current_project_yaml(self, config_file: str = "basis.yml") -> Dict:
    #     return load_yaml(config_file)

    # def get_current_project(self, config_file: str = "basis.yml") -> ProjectCfg:
    #     return ProjectCfg.parse_file(config_file)

    # def write_current_project(
    #     self, project: ProjectCfg, config_file: str = "basis.yml",
    # ):
    #     with open(config_file, "w") as f:
    #         f.write(dump_yaml(project.dict(exclude_unset=True)))

