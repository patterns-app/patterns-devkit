from __future__ import annotations

import os
from pathlib import Path
import sys
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Union,
)

from commonmodel.base import Schema, SchemaLike, schema_from_yaml
from loguru import logger
from snapflow.core.component import ComponentLibrary, DictView

if TYPE_CHECKING:
    from snapflow.core.snap import (
        SnapLike,
        _Snap,
        make_snap,
    )
    from snapflow.core.snap_package import SnapPackage

DEFAULT_LOCAL_NAMESPACE = "_local"
DEFAULT_NAMESPACE = DEFAULT_LOCAL_NAMESPACE


class ModuleException(Exception):
    pass


class SnapflowModule:
    namespace: str
    py_module_path: Optional[str]
    py_namespace: Optional[str]
    snap_paths: List[str] = ["snaps"]
    schema_paths: List[str] = ["schemas"]
    library: ComponentLibrary
    dependencies: List[SnapflowModule]

    def __init__(
        self,
        namespace: str,
        py_module_path: Optional[str] = None,
        py_namespace: Optional[str] = None,
        snap_paths: List[str] = ["snaps"],
        schema_paths: List[str] = ["schemas"],
        dependencies: List[
            SnapflowModule
        ] = None,  # TODO: support str references to external deps (will need repo hooks...)
    ):

        self.namespace = namespace
        if py_module_path:
            py_module_path = os.path.dirname(py_module_path)
        self.py_module_path = py_module_path
        self.py_namespace = py_namespace
        self.library = ComponentLibrary(namespace_lookup_keys=[self.namespace])
        self.snap_paths = snap_paths
        self.schema_paths = schema_paths
        self.dependencies = []
        self.snap_packages = {}
        if self.py_module_path:
            self.discover_schemas()
            self.discover_snaps()
        for d in dependencies or []:
            self.add_dependency(d)
        # for t in tests or []:
        #     self.add_test_case(t)

    def discover_snaps(self):
        if not self.py_module_path:
            return
        from snapflow.core.snap_package import SnapPackage

        for snaps_path in self.snap_paths:
            snaps_root = Path(self.py_module_path).resolve() / snaps_path
            packages = SnapPackage.all_from_root_path(
                str(snaps_root), namespace=self.namespace
            )
            for pkg in packages:
                self.snap_packages[pkg.name] = pkg
                self.library.add_snap(pkg.snap)

    def discover_schemas(self):
        if not self.py_module_path:
            return
        for schemas_path in self.schema_paths:
            schemas_root = Path(self.py_module_path).resolve() / schemas_path
            for fname in os.listdir(schemas_root):
                if fname.endswith(".yml") or fname.endswith(".yaml"):
                    with open(schemas_root / fname) as f:
                        yml = f.read()
                        self.add_schema(yml)

    def add_snap_package(self, pkg: SnapPackage):
        self.snap_packages[pkg.name] = pkg
        self.add_snap(pkg.snap)

    def add_snap(self, snap_like: Union[SnapLike, str]) -> _Snap:
        p = self.process_snap(snap_like)
        self.validate_key(p)
        self.library.add_snap(p)
        return p

    def add_schema(self, schema_like: SchemaLike) -> Schema:
        schema = self.process_schema(schema_like)
        self.validate_key(schema)
        self.library.add_schema(schema)
        return schema

    def process_snap(self, snap_like: Union[SnapLike, str, ModuleType]) -> _Snap:
        from snapflow.core.snap import _Snap, make_snap, PythonCodeSnapWrapper
        from snapflow.core.sql.sql_snap import sql_snap
        from snapflow.core.snap_package import SnapPackage

        if isinstance(snap_like, _Snap):
            snap = snap_like
        else:
            if callable(snap_like):
                snap = make_snap(snap_like, namespace=self.namespace)
            # elif isinstance(snap_like, str):
            #     # Just a string, not a sql file, assume it is python? TODO
            #     snap = make_snap(PythonCodeSnapWrapper(snap_like), module=self.name)
            elif isinstance(snap_like, ModuleType):
                # Module snap (the new default)
                pkg = SnapPackage.from_module(snap_like)
                self.add_snap_package(pkg)
                return pkg.snap
                # code = inspect.getsource(snap_like)
                # snap = make_snap(PythonCodeSnapWrapper(code), module=self.name)
            else:
                raise TypeError(snap_like)
        return snap

    def process_schema(self, schema_like: SchemaLike) -> Schema:
        if isinstance(schema_like, Schema):
            schema = schema_like
        elif isinstance(schema_like, str):
            schema = schema_from_yaml(schema_like, namespace=self.namespace)
        else:
            raise TypeError(schema_like)
        return schema

    def get_schema(self, schema_like: SchemaLike) -> Schema:
        if isinstance(schema_like, Schema):
            return schema_like
        return self.library.get_schema(schema_like)

    def get_snap(self, snap_like: Union[_Snap, str]) -> _Snap:
        from snapflow.core.snap import _Snap

        if isinstance(snap_like, _Snap):
            return snap_like
        return self.library.get_snap(snap_like)

    def export(self):
        if self.py_namespace is None:
            raise Exception("Cannot export module, no namespace set")
        mod = sys.modules[
            self.py_namespace
        ]  # = self  # type: ignore  # sys.module_lookup_names wants a modulefinder.Module type and it's not gonna get it
        setattr(mod, "snaps", self.snaps)
        setattr(mod, "schemas", self.schemas)

    @property
    def schemas(self) -> DictView[str, Schema]:
        return self.library.get_schemas_view()

    @property
    def snaps(self) -> DictView[str, _Snap]:
        return self.library.get_snaps_view()

    def validate_key(self, obj: Any):
        if hasattr(obj, "namespace"):
            if obj.namespace != self.namespace:
                raise ModuleException(
                    f"Component {obj} namespace `{obj.namespace}` does not match module namespace `{self.namespace}` to which it was added"
                )

    def remove_snap(self, snap_like: Union[SnapLike, str]):
        self.library.remove_snap(snap_like)

    def run_tests(self):
        from snapflow.testing.utils import run_test_case, TestFeatureNotImplementedError

        for name, pkg in self.snap_packages.items():
            print(f"Running tests for snap {name}")
            for case in pkg.get_test_cases():
                print(f"======= {case.name} =======")
                try:
                    run_test_case(case)
                except TestFeatureNotImplementedError as e:
                    logger.warning(f"Test feature not implemented yet {e.args[0]}")
                except Exception as e:
                    import traceback

                    print(traceback.format_exc())
                    print(e)
                    raise e

    def add_dependency(self, m: SnapflowModule):
        # if isinstance(m, SnapflowModule):
        #     m = m.name
        self.dependencies.append(m)


DEFAULT_LOCAL_MODULE = SnapflowModule(DEFAULT_LOCAL_NAMESPACE)
