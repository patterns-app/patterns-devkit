from __future__ import annotations

import os
from snapflow.core.snap import Snap, _Snap, ensure_snap, make_snap
import sys
from importlib import import_module
from pathlib import Path
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
    cast,
)
from types import ModuleType

from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.module import (
    DEFAULT_LOCAL_MODULE,
    DEFAULT_LOCAL_NAMESPACE,
    SnapflowModule,
)


if TYPE_CHECKING:
    from snapflow.testing.utils import TestCase


class NoSnapFoundError(Exception):
    pass


@dataclass  # (frozen=True)
class SnapPackage:
    name: str
    root_path: str
    snap: _Snap
    # local_vars: Dict = None
    # root_module: ModuleType
    namespace: str = None
    tests: List[Dict] = field(default_factory=list)
    snap_python_name: str = None
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
    ) -> SnapPackage:
        pth = Path(abs_path)
        name = pth.parts[-1]
        python_path = str(pth / (name + ".py"))
        # local_vars = load_python_file(python_path, __file__=python_path)
        m = load_module(pth / name)
        snap = getattr(m, name)
        # snap = local_vars.get(name)
        # if snap is None:
        #     snaps = [v for v in local_vars.values() if isinstance(v, _Snap)]
        #     if not snaps:
        #         raise NoSnapFoundError
        #     assert len(snaps) == 1, "One snap per file"
        #     snap = snaps[0]
        snap = make_snap(snap)
        if namespace:
            snap.namespace = namespace
        pkg = SnapPackage(
            name=name,
            root_path=abs_path,
            namespace=namespace,
            snap=snap,
            # local_vars=local_vars,
        )
        pkg.find_files()
        pkg.find_tests()
        return pkg

    # @classmethod
    # def create_from_module(cls, root_module: ModuleType) -> SnapPackage:
    #     try:
    #         tests_package = import_module(".tests", package=root_module.__name__)
    #         print(tests_package)
    #         print(dir(tests_package))
    #     except ImportError as e:
    #         pass
    #     snap_module = import_module(
    #         "." + root_module.__name__, package=root_module.__name__
    #     )
    #     snaps = load_snaps_from_module(snap_module)
    #     if not snaps:
    #         raise NoSnapFoundError
    #     assert len(snaps) == 1
    #     return SnapPackage(
    #         name=snap_module.__name__,
    #         root_path=root_module.__file__,
    #         root_module=root_module,
    #         snap=snaps[0],
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
    def from_snap(cls, snap: _Snap, **overrides: Any):
        # TODO: probably not always right...?
        root_path = Path(
            sys.modules[snap.get_original_object().__module__].__file__
        ).resolve()
        args = dict(
            name=snap.name,
            root_path=root_path,
            namespace=snap.namespace,
            display_name=snap.display_name,
            short_description=snap.description,
            snap=snap,
        )
        args.update(overrides)
        pkg = SnapPackage(**args)
        snap.package = pkg
        return pkg

    @classmethod
    def from_module(cls, module: ModuleType, **overrides: Any) -> SnapPackage:
        # TODO: better to just load straight from module?
        return SnapPackage.from_path(module.__file__, **overrides)

    # @classmethod
    # def all_from_root_module(
    #     cls, module: ModuleType, snaps_module_path="snaps", **overrides: Any
    # ) -> List[SnapPackage]:
    #     # TODO: better to just load straight from module?
    #     pckgs = []
    #     snaps_module = getattr(module, snaps_module_path)
    #     print(dir(module))
    #     print(dir(snaps_module))
    #     for name in dir(snaps_module):
    #         obj = getattr(snaps_module, name)
    #         if isinstance(obj, ModuleType):
    #             try:
    #                 pckgs.append(SnapPackage.from_module(obj))
    #             except NoSnapFoundError:
    #                 pass
    #     return pckgs

    @classmethod
    def all_from_root_path(
        cls, path: str, namespace: str = None, **overrides: Any
    ) -> List[SnapPackage]:
        # TODO: better to just load straight from module?
        pkgs = []
        for f in os.scandir(path):
            if not f.is_dir():
                continue
            try:
                pkg = SnapPackage.from_path(f.path, namespace=namespace)
                pkgs.append(pkg)
            except (NoSnapFoundError, ModuleNotFoundError) as e:
                raise e
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


def load_snaps_from_file(pth: str, **local_vars: Any) -> List[_Snap]:
    objects = load_python_file(pth, **local_vars)
    return [v for v in objects.values() if isinstance(v, _Snap)]


def load_snaps_from_module(module: ModuleType) -> List[_Snap]:
    return [
        getattr(module, n) for n in dir(module) if isinstance(getattr(module, n), _Snap)
    ]


def get_module_abs_path(obj: Any) -> Path:
    return Path(sys.modules[obj.__module__].__file__).resolve()


def load_file(file_path: str, fname: str) -> str:
    pth = Path(file_path).resolve() / fname
    return open(pth).read()
