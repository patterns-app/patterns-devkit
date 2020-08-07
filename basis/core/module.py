from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from io import TextIOBase
from typing import (
    TYPE_CHECKING,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    TextIO,
    Type,
    Union,
)

from basis.core.component import (
    ComponentLibrary,
    ComponentType,
    ComponentUri,
    ComponentView,
)
from basis.core.typing.object_type import ObjectType, ObjectTypeLike, otype_from_yaml

if TYPE_CHECKING:
    from basis.core.pipe import (
        PipeLike,
        Pipe,
        make_pipe_definition,
        PipeDefinition,
    )
    from basis.testing.pipes import PipeTest


class BasisModule:
    name: str
    py_module_path: Optional[str]
    py_module_name: Optional[str]
    library: ComponentLibrary
    test_cases: List[PipeTest]
    dependencies: List[BasisModule]

    def __init__(
        self,
        name: str,
        py_module_path: Optional[str] = None,
        py_module_name: Optional[str] = None,
        otypes: Optional[Sequence[ObjectTypeLike]] = None,
        pipes: Optional[Sequence[Union[PipeLike, str]]] = None,
        tests: Optional[Sequence[PipeTest]] = None,
        dependencies: List[
            BasisModule
        ] = None,  # TODO: support str references to external deps (will need repo hooks...)
    ):

        self.name = name
        if py_module_path:
            py_module_path = os.path.dirname(py_module_path)
        self.py_module_path = py_module_path
        self.py_module_name = py_module_name
        self.library = ComponentLibrary(default_module=self)
        self.test_cases = []
        self.dependencies = []
        for otype in otypes or []:
            self.add_otype(otype)
        for fn in pipes or []:
            self.add_pipe(fn)
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
    def open_module_file(self, fp: str) -> Generator[TextIO, None, None]:
        if not self.py_module_path:
            raise Exception(f"Module path not set, cannot read {fp}")
        typedef_path = os.path.join(self.py_module_path, fp)
        with open(typedef_path) as f:
            yield f

    def get_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            return otype_like
        return self.library.get_otype(otype_like)

    def get_pipe(self, df_like: Union[Pipe, PipeDefinition, str]) -> Pipe:
        from basis.core.pipe import (
            Pipe,
            make_pipe_definition,
        )

        if isinstance(df_like, Pipe):
            return df_like
        return self.library.get_pipe(df_like)

    def export(self):
        if self.py_module_name is None:
            raise Exception("Cannot export module, no module_name set")
        sys.modules[self.py_module_name] = self  # type: ignore  # sys.modules wants a modulefinder.Module type and it's not gonna get it

    def validate_component(self, component: ComponentUri):
        assert component.module_name == self.name

    @property
    def otypes(self) -> ComponentView[ObjectType]:
        return self.library.component_view(ctype=ComponentType.ObjectType)

    @property
    def pipes(self) -> ComponentView[ObjectType]:
        return self.library.component_view(ctype=ComponentType.Pipe)

    def add_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        otype = self.process_otype(otype_like)
        self.library.add_component(otype)
        return otype

    def process_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            otype = otype_like
        elif isinstance(otype_like, str):
            with self.open_module_file(otype_like) as f:
                yml = f.read()
                otype = otype_from_yaml(yml, module_name=self.name)
        else:
            raise TypeError(otype_like)
        return otype

    def add_pipe(self, df_like: Union[PipeLike, str]) -> Pipe:
        fn = self.process_pipe(df_like)
        self.library.add_component(fn)
        return fn

    def process_pipe(self, df_like: Union[PipeLike, str]) -> Pipe:
        from basis.core.pipe import (
            PipeDefinition,
            make_pipe_definition,
            Pipe,
        )
        from basis.core.sql.pipe import sql_pipe

        if isinstance(df_like, Pipe):
            df = df_like
        else:
            dfd = None
            if isinstance(df_like, PipeDefinition):
                dfd = df_like
            elif callable(df_like):
                dfd = make_pipe_definition(df_like, module_name=self.name)
            elif isinstance(df_like, str) and df_like.endswith(".sql"):
                if not self.py_module_path:
                    raise Exception(
                        f"Module path not set, cannot read sql definition {df_like}"
                    )
                sql_file_path = os.path.join(self.py_module_path, df_like)
                with open(sql_file_path) as f:
                    sql = f.read()
                file_name = os.path.basename(df_like)[:-4]
                dfd = sql_pipe(
                    name=file_name, sql=sql, module_name=self.name
                )  # TODO: versions, runtimes, etc for sql (someway to specify in a .sql file)
            else:
                raise Exception(f"Invalid Pipe {df_like}")
            df = dfd.as_pipe()
        return df

    def add_test(self, test_case: PipeTest):
        test_case.module = self
        self.test_cases.append(test_case)

    def run_tests(self):
        print(f"Running tests for module {self.name}")
        for test in self.test_cases:
            print(f"======= {test.pipe} =======")
            try:
                test.run(initial_modules=[self] + self.dependencies)
            except Exception as e:
                print(e)
                raise e

    def add_dependency(self, m: BasisModule):
        # if isinstance(m, BasisModule):
        #     m = m.name
        self.dependencies.append(m)

    # def add_test_case(self, test_case_like: PipeTestCaseLike):
    #     test_case = self.process_test_case(test_case_like)
    #     self.test_cases.extend(test_case)
    #
    # def process_test_case(
    #     self, test_case_like: PipeTestCaseLike
    # ) -> List[PipeTestCase]:
    #     from basis.testing.pipes import (
    #         PipeTestCaseLike,
    #         PipeTestCase,
    #         test_cases_from_yaml,
    #     )
    #
    #     if isinstance(test_case_like, PipeTestCase):
    #         test_cases = [test_case_like]
    #     elif isinstance(test_case_like, str):
    #         yml = self.read_module_file(test_case_like)
    #         test_cases = test_cases_from_yaml(yml, self)
    #     else:
    #         raise TypeError(test_case_like)
    #     return test_cases

    # def get_indexable_components(self) -> Iterable[IndexableComponent]:
    #     dti = self.otype_indexer()
    #     si = self.provider_indexer()
    #     dfi = self.pipe_indexer()
    #     for otype in self.otypes.all():
    #         for ic in dti.get_indexable_components(otype, self):
    #             yield ic
    #     for s in self.providers.all():
    #         for ic in si.get_indexable_components(s, self):
    #             yield ic
    #     for df in self.pipes.all():
    #         for ic in dfi.get_indexable_components(df, self):
    #             yield ic


DEFAULT_LOCAL_MODULE = BasisModule("_local_")
