from __future__ import annotations

import os
from pathlib import Path

from cleo import Command
from snapflow.templates.generator import generate_template


def strip_snapflow(s: str) -> str:
    if s.startswith("snapflow_"):
        return s[9:]
    return s


class GenerateCommand(Command):
    """
    Generate new snapflow component

    new
        {type : Type of component to generate, one of (module, function, schema)}
        {name : name of the component }
        {--s|namespace : namespace of the component, defaults to current module namespace }
    """

    def handle(self):
        type_ = self.argument("type")
        name = self.argument("name")
        namespace = self.option("namespace")
        if type_ == "module":
            self.handle_module(name, namespace)
        elif type_ == "function":
            self.handle_function(name, namespace)
        elif type_ == "schema":
            raise NotImplementedError
            self.handle_schema(name, namespace)
        else:
            raise ValueError(
                f"Invalid type {type_}, must be one of (module, function, schema)"
            )

    def handle_module(self, name: str, namespace: str):
        py_module_name = name
        name = strip_snapflow(name)
        namespace = strip_snapflow(namespace or name)
        generate_template(
            "module",
            py_module_name=py_module_name,
            namespace=namespace,
            module_name=name,
        )
        generate_template("tests", py_module_name=py_module_name, module_name=name)

    def handle_function(self, name: str, namespace: str):
        namespace = strip_snapflow(namespace or self.get_current_snapflow_module_name())
        os.chdir(self.abs_path("functions"))
        generate_template("function", function_name=name, namespace=namespace)

    def handle_schema(self, name: str, namespace: str):
        namespace = strip_snapflow(namespace or self.get_current_snapflow_module_name())
        os.chdir(self.abs_path("schemas"))
        generate_template("schema", schema_name=name, namespace=namespace)

    def get_current_py_module_name(self) -> str:
        project_dir = self.get_current_dir()
        py_module_name = project_dir.replace("-", "_")
        return py_module_name

    def get_current_snapflow_module_name(self) -> str:
        py_module_name = self.get_current_py_module_name()
        return strip_snapflow(py_module_name)

    def abs_path(self, pth: str) -> str:
        return str(Path(self.get_current_py_module_name()) / pth)

    def get_current_dir(self) -> str:
        return Path(os.getcwd()).parts[-1]
