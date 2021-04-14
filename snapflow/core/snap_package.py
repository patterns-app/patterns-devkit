from __future__ import annotations

import os
from snapflow.core.snap import _Snap
import sys
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
    DEFAULT_LOCAL_MODULE_NAME,
    SnapflowModule,
)


class NoSnapFoundError(Exception):
    pass


@dataclass  # (frozen=True)
class SnapPackage:
    name: str
    root_path: str
    snap: _Snap
    module_name: str = None
    python_path: str = None
    snap_python_name: str = None
    readme_path: str = "README.md"
    test_data_path: str = None
    python_requirements_path: str = None
    docker_file_path: str = None
    display_name: Optional[str] = None
    short_description: Optional[str] = None

    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    @classmethod
    def from_path(
        cls, abs_path: str, module_name: str = None, **overrides: Any
    ) -> SnapPackage:
        pth = Path(abs_path)
        args = dict(name=pth.parts[-1], module_name=module_name, root_path=abs_path)
        for fname in os.listdir(pth):
            if fname.lower().startswith("readme"):
                args["readme_path"] = str(pth / fname)
            elif fname.lower().endswith(".py"):
                if fname[:-3] == args["name"]:
                    args["python_path"] = str(pth / fname)
            elif fname.lower() == "requirements.txt":
                args["python_requirements_path"] = str(pth / fname)
        args.update(overrides)
        snaps = load_snaps_from_file(args["python_path"], __file__=args["root_path"])
        if not snaps:
            raise NoSnapFoundError
        assert len(snaps) == 1, "One snap per file"
        snap = snaps[0]
        args["snap"] = snap
        return SnapPackage(**args)

    @classmethod
    def from_snap(cls, snap: _Snap, **overrides: Any):
        # TODO: probably not always right...?
        root_path = Path(
            sys.modules[snap.get_original_object().__module__].__file__
        ).resolve()
        args = dict(
            name=snap.name,
            root_path=root_path,
            module_name=snap.module_name,
            display_name=snap.display_name,
            short_description=snap.description,
            snap=snap,
        )
        args.update(overrides)
        mfs = SnapPackage(**args)
        return mfs

    @classmethod
    def from_module(cls, module: ModuleType, **overrides: Any) -> SnapPackage:
        # TODO: better to just load straight from module?
        return SnapPackage.from_path(module.__file__, **overrides)

    @classmethod
    def all_from_root_module(
        cls, module: ModuleType, snaps_module_path="snaps", **overrides: Any
    ) -> List[SnapPackage]:
        # TODO: better to just load straight from module?
        pckgs = []
        snaps_module = getattr(module, snaps_module_path)
        print(dir(module))
        print(dir(snaps_module))
        for name in dir(snaps_module):
            obj = getattr(snaps_module, name)
            if isinstance(obj, ModuleType):
                try:
                    pckgs.append(SnapPackage.from_module(obj))
                except NoSnapFoundError:
                    pass
        return pckgs

    def load_python(self) -> Dict[str, Any]:
        return load_python_file(self.python_path, __file__=self.root_path)

    def load_readme(self) -> str:
        return open(self.readme_path).read()

    def abs_path(self, fname: str) -> str:
        return str(Path(self.root_path) / fname)


def load_python_file(pth: str, **local_vars: Any) -> Dict[str, Any]:
    objects = {}
    objects.update(local_vars)
    exec(open(pth).read(), globals(), objects)
    return objects


def load_snaps_from_file(pth: str, **local_vars: Any) -> List[_Snap]:
    objects = load_python_file(pth, **local_vars)
    return [v for v in objects.values() if isinstance(v, _Snap)]


def get_module_abs_path(obj: Any) -> Path:
    return Path(sys.modules[obj.__module__].__file__).resolve()


def load_file(file_path: str, fname: str) -> str:
    pth = Path(file_path).resolve() / fname
    return open(pth).read()
