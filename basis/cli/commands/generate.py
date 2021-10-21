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

    generate
        {type : What to generate, one of [graph, node]}
        {path? : Path to generate at}
    """

    supported_types = ["graph", "node"]

    def handle(self):
        type_ = self.argument("type")
        destination_path = self.argument("path") or "."
        if type_ not in self.supported_types:
            raise ValueError(
                f"Invalid type {type_}, must be one of {self.supported_types}"
            )

        cfg = getattr(self, f"{type_}_config_from_dialogue")(destination_path)
        if "template_name" not in cfg:
            cfg["template_name"] = f"{type_}"
        if "destination_path" not in cfg:
            cfg["destination_path"] = destination_path
        generate_template(**cfg)
        self.line(f"<info>Successfully created {type_} {destination_path}</info>")

    ### Dialogues

    def graph_config_from_dialogue(self, destination_path: str):
        name = Path(destination_path).name
        name = self.ask(f"Graph name [{name}]:", name)
        return {"graph_name": name}

    def node_config_from_dialogue(self, destination_path: str):
        name = Path(destination_path).name
        name = name.split(".")[0]
        name = self.ask(f"Node name [{name}]:", name)
        langs = ["sql", "python"]
        lang_q = self.create_question(f"Node language [sql, python]:", default="python")
        lang_q.set_autocomplete_values(langs)
        lang = self.ask(lang_q)
        complexity = "simple"
        return {
            "template_name": f"nodes/{lang}/node_{complexity}_{lang}_template",
            "destination_path": Path(destination_path).parent / name,
            "flatten": True,
            "node_name": name,
        }
