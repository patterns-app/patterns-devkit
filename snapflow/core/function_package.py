from __future__ import annotations

import os
import sys
from dataclasses import asdict, dataclass, field
from functools import partial
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.function import DataFunction, ensure_function, make_function
from snapflow.core.module import (
    DEFAULT_LOCAL_MODULE,
    DEFAULT_LOCAL_NAMESPACE,
    SnapflowModule,
)

if TYPE_CHECKING:
    from snapflow.testing.utils import TestCase


class NoDataFunctionFoundError(Exception):
    pass


@dataclass  # (frozen=True)
class DataFunctionPackage:
    name: str
    root_path: str
    function: DataFunction
    # local_vars: Dict = None
    # root_module: ModuleType
    namespace: str = None
    tests: List[Dict] = field(default_factory=list)
    function_python_name: str = None
    readme_name: str = "README.md"
    # python_requirements_path: str = None
    # docker_file_path: str = None
    display_name: Optional[str] = None
    short_description: Optional[str] = None

    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    @classmethod
    def from_path(
        cls, abs_path: str, namespace: str = None, **overrides: Any
    ) -> DataFunctionPackage:
        pth = Path(abs_path)
        name = pth.parts[-1]
        # python_path = str(pth / (name + ".py"))
        # local_vars = load_python_file(python_path, __file__=python_path)
        m = load_module(pth / name)
        function = getattr(m, name)
        # function = local_vars.get(name)
        # if function is None:
        #     functions = [v for v in local_vars.values() if isinstance(v, DataFunction)]
        #     if not functions:
        #         raise NoDataFunctionFoundError
        #     assert len(functions) == 1, "One function per file"
        #     function = functions[0]
        function = make_function(function)
        if namespace:
            function.namespace = namespace
        pkg = DataFunctionPackage(
            name=name,
            root_path=abs_path,
            namespace=namespace,
            function=function,
            # local_vars=local_vars,
        )
        pkg.find_files()
        pkg.find_tests()
        return pkg

    # @classmethod
    # def create_from_module(cls, root_module: ModuleType) -> DataFunctionPackage:
    #     try:
    #         tests_package = import_module(".tests", package=root_module.__name__)
    #         print(tests_package)
    #         print(dir(tests_package))
    #     except ImportError as e:
    #         pass
    #     function_module = import_module(
    #         "." + root_module.__name__, package=root_module.__name__
    #     )
    #     functions = load_functions_from_module(function_module)
    #     if not functions:
    #         raise NoDataFunctionFoundError
    #     assert len(functions) == 1
    #     return DataFunctionPackage(
    #         name=function_module.__name__,
    #         root_path=root_module.__file__,
    #         root_module=root_module,
    #         function=functions[0],
    #     )

    def find_files(self):
        for fname in os.listdir(self.root_path):
            if fname.lower().startswith("readme"):
                self.readme_name = fname
            # elif fname.lower().endswith(".py"):
            #     if fname[:-3] == args["name"]:
            #         args["python_path"] = str(pth / fname)
            # elif fname.lower() == "requirements.txt":
            #     self.python_requirements_name = str(pth / fname)

    def find_tests(self):
        tests_path = self.abs_path("tests")
        if not os.path.exists(tests_path):
            return
        tests = []
        for fname in os.listdir(tests_path):
            if fname.startswith("test") and fname.endswith(".py"):
                py_objects = load_python_file(str(Path(tests_path) / fname))
                tests.append(py_objects)
        self.tests = tests

    def get_test_cases(self) -> List[TestCase]:
        from snapflow.testing.utils import TestCase

        cases = []
        for test in self.tests:
            cases.append(TestCase.from_test(test, self))
        return cases

    @classmethod
    def from_function(cls, function: DataFunction, **overrides: Any):
        # TODO: probably not always right...?
        root_path = Path(
            sys.modules[function.get_original_object().__module__].__file__
        ).resolve()
        args = dict(
            name=function.name,
            root_path=root_path,
            namespace=function.namespace,
            display_name=function.display_name,
            short_description=function.description,
            function=function,
        )
        args.update(overrides)
        pkg = DataFunctionPackage(**args)
        function.package = pkg
        return pkg

    @classmethod
    def from_module(cls, module: ModuleType, **overrides: Any) -> DataFunctionPackage:
        # TODO: better to just load straight from module?
        return DataFunctionPackage.from_path(module.__file__, **overrides)

    # @classmethod
    # def all_from_root_module(
    #     cls, module: ModuleType, functions_module_path="functions", **overrides: Any
    # ) -> List[DataFunctionPackage]:
    #     # TODO: better to just load straight from module?
    #     pckgs = []
    #     functions_module = getattr(module, functions_module_path)
    #     print(dir(module))
    #     print(dir(functions_module))
    #     for name in dir(functions_module):
    #         obj = getattr(functions_module, name)
    #         if isinstance(obj, ModuleType):
    #             try:
    #                 pckgs.append(DataFunctionPackage.from_module(obj))
    #             except NoDataFunctionFoundError:
    #                 pass
    #     return pckgs

    @classmethod
    def all_from_root_path(
        cls, path: str, namespace: str = None, **overrides: Any
    ) -> List[DataFunctionPackage]:
        # TODO: better to just load straight from module?
        pkgs = []
        for f in os.scandir(path):
            if not f.is_dir():
                continue
            if f.name.startswith("__"):
                continue
            try:
                pkg = DataFunctionPackage.from_path(f.path, namespace=namespace)
                pkgs.append(pkg)
            except (NoDataFunctionFoundError, ModuleNotFoundError):
                pass
        return pkgs

    def load_readme(self) -> str:
        return open(self.abs_path(self.readme_name)).read()

    def abs_path(self, fname: str) -> str:
        return str(Path(self.root_path) / fname)


def load_python_file(pth: str, **local_vars: Any) -> Dict[str, Any]:
    objects = {}
    objects.update(local_vars)
    exec(open(pth).read(), globals(), objects)
    return objects


def load_module(pth: Path) -> ModuleType:
    name = pth.parts[-1]
    parent = str(pth.parent)
    if parent in sys.path:
        exists = True
    else:
        exists = False
        sys.path.append(parent)
    try:
        return import_module(name)
    finally:
        if not exists:
            sys.path.remove(parent)


def load_functions_from_file(pth: str, **local_vars: Any) -> List[DataFunction]:
    objects = load_python_file(pth, **local_vars)
    return [v for v in objects.values() if isinstance(v, DataFunction)]


def load_functions_from_module(module: ModuleType) -> List[DataFunction]:
    return [
        getattr(module, n)
        for n in dir(module)
        if isinstance(getattr(module, n), DataFunction)
    ]


def get_module_abs_path(obj: Any) -> Path:
    return Path(sys.modules[obj.__module__].__file__).resolve()


def load_file(file_path: str, fname: str) -> str:
    pth = Path(file_path).resolve() / fname
    return open(pth).read()
