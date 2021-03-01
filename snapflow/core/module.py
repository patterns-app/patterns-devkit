from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from io import TextIOBase
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    TextIO,
    Type,
    Union,
)

from snapflow.core.component import ComponentLibrary
from snapflow.schema.base import Schema, SchemaLike, schema_from_yaml
from snapflow.utils.common import AttrDict

if TYPE_CHECKING:
    from snapflow.core.snap import (
        SnapLike,
        _Snap,
        make_snap,
    )

DEFAULT_LOCAL_MODULE_NAME = "_local_"


class ModuleException(Exception):
    pass


class SnapflowModule:
    name: str
    py_module_path: Optional[str]
    py_module_name: Optional[str]
    library: ComponentLibrary
    test_cases: List[Callable]
    dependencies: List[SnapflowModule]

    def __init__(
        self,
        name: str,
        py_module_path: Optional[str] = None,
        py_module_name: Optional[str] = None,
        schemas: Optional[Sequence[SchemaLike]] = None,
        snaps: Optional[Sequence[Union[SnapLike, str]]] = None,
        tests: Optional[Sequence[Callable]] = None,
        dependencies: List[
            SnapflowModule
        ] = None,  # TODO: support str references to external deps (will need repo hooks...)
    ):

        self.name = name
        if py_module_path:
            py_module_path = os.path.dirname(py_module_path)
        self.py_module_path = py_module_path
        self.py_module_name = py_module_name
        self.library = ComponentLibrary(module_lookup_keys=[self.name])
        self.test_cases = []
        self.dependencies = []
        for schema in schemas or []:
            self.add_schema(schema)
        for fn in snaps or []:
            self.add_snap(fn)
        for t in tests or []:
            self.add_test(t)
        for d in dependencies or []:
            self.add_dependency(d)
        # for t in tests or []:
        #     self.add_test_case(t)

    # TODO: implement dir for usability
    # def __dir__(self):
    #     return list(self.members().keys())

    @contextmanager
    def open_module_file(self, fp: str) -> Iterator[TextIO]:
        if not self.py_module_path:
            raise Exception(f"Module path not set, cannot read {fp}")
        typedef_path = os.path.join(self.py_module_path, fp)
        with open(typedef_path) as f:
            yield f

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
        if self.py_module_name is None:
            raise Exception("Cannot export module, no module_name set")
        sys.modules[self.py_module_name] = self  # type: ignore  # sys.module_lookup_names wants a modulefinder.Module type and it's not gonna get it

    @property
    def schemas(self) -> AttrDict[str, Schema]:
        return self.library.get_schemas_view()

    @property
    def snaps(self) -> AttrDict[str, _Snap]:
        return self.library.get_snaps_view()

    def validate_key(self, obj: Any):
        if obj.module_name != self.name:
            raise ModuleException(
                f"Component {obj} module name `{obj.module_name}` does not match module `{self.name}` to which it was added"
            )

    def add_schema(self, schema_like: SchemaLike) -> Schema:
        schema = self.process_schema(schema_like)
        self.validate_key(schema)
        self.library.add_schema(schema)
        return schema

    def process_schema(self, schema_like: SchemaLike) -> Schema:
        if isinstance(schema_like, Schema):
            schema = schema_like
        elif isinstance(schema_like, str):
            with self.open_module_file(schema_like) as f:
                yml = f.read()
                schema = schema_from_yaml(yml, module_name=self.name)
        else:
            raise TypeError(schema_like)
        return schema

    def add_snap(self, snap_like: Union[SnapLike, str]) -> _Snap:
        p = self.process_snap(snap_like)
        self.validate_key(p)
        self.library.add_snap(p)
        return p

    def process_snap(self, snap_like: Union[SnapLike, str]) -> _Snap:
        from snapflow.core.snap import (
            _Snap,
            make_snap,
            _Snap,
        )
        from snapflow.core.sql.sql_snap import sql_snap

        if isinstance(snap_like, _Snap):
            snap = snap_like
        else:
            if callable(snap_like):
                snap = make_snap(snap_like, module=self.name)
            elif isinstance(snap_like, str) and snap_like.endswith(".sql"):
                if not self.py_module_path:
                    raise Exception(
                        f"Module path not set, cannot read sql definition {snap_like}"
                    )
                sql_file_path = os.path.join(self.py_module_path, snap_like)
                with open(sql_file_path) as f:
                    sql = f.read()
                file_name = os.path.basename(snap_like)[:-4]
                snap = sql_snap(
                    name=file_name, module=self.name, sql=sql
                )  # TODO: versions, runtimes, etc for sql (someway to specify in a .sql file)
            else:
                raise TypeError(snap_like)
        return snap

    def add_test(self, test_case: Callable):
        self.test_cases.append(test_case)

    def run_tests(self):
        print(f"Running tests for module {self.name}")
        for test in self.test_cases:
            print(f"======= {test.__name__} =======")
            try:
                test()
            except Exception as e:
                import traceback

                print(traceback.format_exc())
                print(e)
                raise e

    def add_dependency(self, m: SnapflowModule):
        # if isinstance(m, SnapflowModule):
        #     m = m.name
        self.dependencies.append(m)


DEFAULT_LOCAL_MODULE = SnapflowModule(DEFAULT_LOCAL_MODULE_NAME)
