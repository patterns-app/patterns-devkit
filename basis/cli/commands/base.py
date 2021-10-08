from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Pattern
from basis.configuration.base import dump_yaml, load_yaml
from basis.configuration.project import ProjectCfg

from basis.cli.templates.generator import generate_template, insert_into_file
from cleo import Command


class BasisCommandBase:
    # @contextmanager
    # def chdir_relative(self, pth: str):
    #     curdir = os.path.normpath(os.getcwd())
    #     os.chdir(self.get_current_basis_module_abs_path(pth))
    #     yield
    #     os.chdir(curdir)

    # def get_current_basis_module_names(self) -> List[str]:
    #     modules = []
    #     for dirname in os.listdir(os.curdir):
    #         if os.path.isdir(dirname):
    #             has_init = os.path.exists(Path(dirname) / "__init__.py")
    #             has_other = (
    #                 os.path.exists(Path(dirname) / "functions")
    #                 or os.path.exists(Path(dirname) / "schemas")
    #                 or os.path.exists(Path(dirname) / "flows")
    #             )
    #             if has_init and has_other:
    #                 modules.append(dirname)
    #     return modules

    # def get_current_basis_module_name(self) -> str:
    #     names = self.get_current_basis_module_names()
    #     assert (
    #         len(names) == 1
    #     ), f"Expected one basis module, found {len(names)}: {names}"
    #     return names[0]

    # def import_current_basis_module(self) -> ModuleType:
    #     sys.path.append(".")
    #     mod = import_module(self.get_current_basis_module_name())
    #     sys.path.pop()
    #     return mod

    def get_current_project_yaml(self, config_file: str = "basis.yml") -> Dict:
        return load_yaml(config_file)

    def get_current_project(self, config_file: str = "basis.yml") -> ProjectCfg:
        return ProjectCfg.parse_file(config_file)

    def write_current_project(
        self, project: ProjectCfg, config_file: str = "basis.yml",
    ):
        with open(config_file, "w") as f:
            f.write(dump_yaml(project.dict(exclude_unset=True)))

    # def insert_into_current_init_file(self, insert: str, after: str):
    #     insert_into_file(
    #         self.abs_path(self.get_current_basis_module_init_path()), insert, after
    #     )

    # def insert_function_into_current_init_file(self, function_name: str):
    #     insert = (
    #         f"from .functions.{function_name}.{function_name} import {function_name}"
    #     )
    #     after = r"^(from|import).*"
    #     self.insert_into_current_init_file(insert, after)

    # def insert_schema_into_current_init_file(self, schema_name: str):
    #     insert = f'{schema_name} = schema_from_yaml_file(Path(__file__).parent / "schemas/{schema_name}.yml")'
    #     after = r".*schema_from_yaml.*"
    #     self.insert_into_current_init_file(insert, after)

    # def get_current_py_module_name(self) -> str:
    #     project_dir = self.get_current_dir()
    #     py_module_name = project_dir.replace("-", "_")  # TODO
    #     return py_module_name

    # def get_current_basis_module_name(self) -> str:
    #     py_module_name = self.get_current_py_module_name()
    #     return strip_basis(py_module_name)

    # def abs_path(self, pth: str) -> str:
    #     return str(Path(os.getcwd()) / pth)

    # def get_current_dir(self) -> str:
    #     return Path(os.getcwd()).parts[-1]
