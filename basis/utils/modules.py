from __future__ import annotations

import importlib
import sys
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Type, TypeVar

T = TypeVar("T")


def find_all_of_type_in_module(module: ModuleType, typ: Type[T]) -> list[T]:
    return [
        getattr(module, n) for n in dir(module) if isinstance(getattr(module, n), typ)
    ]


def load_python_file(pth: str, **local_vars: Any) -> dict[str, Any]:
    objects = {}
    objects.update(local_vars)
    exec(open(pth).read(), globals(), objects)
    return objects


def load_module(pth: Path) -> ModuleType:
    parent = str(pth.parent)
    if parent in sys.path:
        exists = True
    else:
        exists = False
        sys.path.insert(0, parent)
    try:
        return import_module(pth.stem)
    finally:
        if not exists:
            sys.path.remove(parent)


def find_single_of_type_in_module(module: ModuleType, typ: Type[T]) -> T:
    objs = find_all_of_type_in_module(module, typ)
    assert (
        len(objs) == 1
    ), f"Module must define one and only one {typ.__name__} (found {len(objs)})"
    return objs[0]


def single_of_type_in_path(pth: str, typ: Type[T]) -> T:
    if Path(pth).exists():
        # It's a file path to a python file ("folder1/folder2/node1.py")
        return find_single_of_type_in_module(load_module(Path(pth)), typ)
    # Otherwise it is a python import path ("pckg1.mod1.node1")
    # First see if path is to python module w function inside
    try:
        mod = importlib.import_module(pth)
        return find_single_of_type_in_module(mod, typ)
    except (ModuleNotFoundError, AssertionError, ValueError):  # TODO: error
        pass
    # Next try as full path to function
    crumbs = pth.split(".")
    try:
        mod_path_parent = ".".join(crumbs[:-1])
        mod = importlib.import_module(mod_path_parent)
        return find_single_of_type_in_module(mod, typ)
    except (ModuleNotFoundError, ValueError):  # TODO: error
        pass
    # Finally look for the name as local (For testing or other purposes?)
    # TODO: is this necessary?
    return globals()[pth]
