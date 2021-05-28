from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import List, Pattern

from cleo import Command
from snapflow.cli.commands.base import SnapflowCommandBase
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.templates.generator import generate_template, insert_into_file


def strip_snapflow(s: str) -> str:
    if s.startswith("snapflow_"):
        return s[9:]
    return s


class GenerateCommand(SnapflowCommandBase, Command):
    """
    Generate new snapflow component

    new
        {type : Type of component to generate (module, dataspace, function, schema, or flow)}
        {name : name of the component }
        {--s|namespace : namespace of the component, defaults to current module namespace }
    """

    def handle(self):
        # self.import_current_snapflow_module()
        type_ = self.argument("type")
        name = self.argument("name")
        namespace = self.option("namespace")
        try:
            getattr(self, f"handle_{type_}")(name, namespace)
        except AttributeError:
            raise ValueError(
                f"Invalid type {type_}, must be one of (module, dataspace, flow, function, schema)"
            )

    def handle_module(self, name: str, namespace: str):
        namespace = namespace or name
        generate_template(
            "module",
            namespace=namespace,
            name=name,
        )
        # generate_template("tests", py_module_name=py_module_name, module_name=name)

    def handle_dataspace(self, name: str, namespace: str):
        name = namespace or name
        generate_template(
            "dataspace",
            name=name,
        )
        # Move single file back down to root (cookiecutter doesn't support)
        os.rename(f"{name}/snapflow.yml", "snapflow.yml")

    def handle_function(self, name: str, namespace: str):
        module = self.import_current_snapflow_module()
        namespace = getattr(module, "namespace", None)
        with self.chdir_relative("functions"):
            generate_template("function", function_name=name, namespace=namespace)
        self.insert_function_into_current_init_file(name)

    def handle_schema(self, name: str, namespace: str):
        namespace = strip_snapflow(namespace or self.get_current_snapflow_module_name())
        with self.chdir_relative("schemas"):
            generate_template("schema", schema_name=name, namespace=namespace)
        self.insert_schema_into_current_init_file(name)

    def handle_flow(self, name: str, namespace: str):
        namespace = strip_snapflow(namespace or self.get_current_snapflow_module_name())
        os.chdir(self.abs_path("flows"))
        generate_template("flow", flow_name=name, namespace=namespace)
