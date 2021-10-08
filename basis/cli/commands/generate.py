from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import List, Pattern

from basis.cli.commands.base import BasisCommandBase
from basis.cli.templates.generator import generate_template
from cleo import Command


class GenerateCommand(BasisCommandBase, Command):
    """
    Generate new basis component

    create
        {type : What to generate, one of [project, app, component]}
    """

    supported_types = ["project", "app", "component"]

    def handle(self):
        type_ = self.argument("type")
        if type_ not in self.supported_types:
            raise ValueError(
                f"Invalid type {type_}, must be one of {self.supported_types}"
            )

        cfg = getattr(self, f"{type_}_config_from_dialogue")()
        generate_template(type_, **cfg)

    ### Dialogues

    def project_config_from_dialogue(self):
        name = self.ask("Project name: ")
        return {"project_name": name}

    def app_config_from_dialogue(self):
        name = self.ask("App name: ")
        return {"app_name": name}

    def component_config_from_dialogue(self):
        name = self.ask("Component name: ")
        return {"component_name": name}
