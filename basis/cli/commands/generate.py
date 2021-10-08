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
        {path : Path to generate at}
    """

    supported_types = ["project", "app", "component"]

    def handle(self):
        type_ = self.argument("type")
        destination_path = self.argument("path")
        if type_ not in self.supported_types:
            raise ValueError(
                f"Invalid type {type_}, must be one of {self.supported_types}"
            )

        cfg = getattr(self, f"{type_}_config_from_dialogue")(destination_path)
        if "template_name" not in cfg:
            cfg["template_name"] = f"{type_}_template"
        if "path" not in cfg:
            cfg["destination_path"] = destination_path
        generate_template(**cfg)
        self.line(f"<info>Successfully created {type_} {destination_path}</info>")

    ### Dialogues

    def project_config_from_dialogue(self, destination_path: str):
        name = Path(destination_path).name
        name = self.ask(f"Project name [{name}]:", name)
        return {"project_name": name}

    def app_config_from_dialogue(self, destination_path: str):
        name = Path(destination_path).name
        name = self.ask(f"App name [{name}]:", name)
        return {"app_name": name}

    def component_config_from_dialogue(self, destination_path: str):
        name = Path(destination_path).name
        name = name.split(".")[0]
        name = self.ask(f"Component name [{name}]:", name)
        langs = ["sql", "python"]
        lang_q = self.create_question(
            f"Component language [sql, python]:", default="python"
        )
        lang_q.set_autocomplete_values(langs)
        lang = self.ask(lang_q)
        complexity = "simple"
        return {
            "template_name": f"component_{complexity}_{lang}_template",
            "destination_path": Path(destination_path).parent / name,
            "flatten": True,
            "component_name": name,
        }
