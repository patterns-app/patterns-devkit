from __future__ import annotations

import os
from pathlib import Path

from cleo import Command
from snapflow.templates.generator import generate_template
import yaml


class RunCommand(Command):
    """
    Run a dataspace or node

    run
        {dataspace : path to dataspace configuration yaml }
        {node? : name of node to run (runs entire dataspace if not specified) }
        {--t|timelimit : per-node execution time limit, in seconds }
        {--s|storage : target storage for execution output }
    """

    def handle(self):
        dataspace_file = self.argument("dataspace")
        node_key = self.argument("node")
        timelimit = self.option("timelimit")

    def load_dataspace_cfg(self, file_name: str) -> DataspaceCfg:
        yml = yaml.load(file_name, Loader=yaml.Loader)
        return DataspaceCfg(**yml)

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

    def handle_dataspace(self, name: str, namespace: str):
        py_module_name = name
        name = strip_snapflow(name)
        namespace = strip_snapflow(namespace or name)
        generate_template(
            "dataspace",
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

    def handle_flow(self, name: str, namespace: str):
        namespace = strip_snapflow(namespace or self.get_current_snapflow_module_name())
        os.chdir(self.abs_path("flows"))
        generate_template("flow", flow_name=name, namespace=namespace)

    def get_current_py_module_name(self) -> str:
        project_dir = self.get_current_dir()
        py_module_name = project_dir.replace("-", "_")  # TODO
        return py_module_name

    def get_current_snapflow_module_name(self) -> str:
        py_module_name = self.get_current_py_module_name()
        return strip_snapflow(py_module_name)

    def abs_path(self, pth: str) -> str:
        return str(Path(self.get_current_py_module_name()) / pth)

    def get_current_dir(self) -> str:
        return Path(os.getcwd()).parts[-1]
