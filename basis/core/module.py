from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Iterable, List, Optional, Sequence, Type, Union

from pandas._testing import assert_almost_equal

from basis.core.component import (
    ComponentLibrary,
    ComponentType,
    ComponentUri,
    ComponentView,
)
from basis.core.typing.object_type import ObjectType, ObjectTypeLike, otype_from_yaml
from basis.utils.common import cf
from basis.utils.pandas import assert_dataframes_are_almost_equal

if TYPE_CHECKING:
    from basis.core.environment import Environment
    from basis.core.data_function import (
        DataFunctionLike,
        DataFunction,
    )
    from basis.core.external import ExternalProvider
    from basis.testing.functions import (
        DataFunctionTestCaseLike,
        DataFunctionTestCase,
        test_case_from_yaml,
        TestCase,
    )


class BasisModule:
    name: str
    py_module_path: Optional[str]
    py_module_name: Optional[str]
    library: ComponentLibrary
    test_cases: List[TestCase]

    def __init__(
        self,
        name: str,
        py_module_path: Optional[str] = None,
        py_module_name: Optional[str] = None,
        otypes: Optional[Sequence[ObjectTypeLike]] = None,
        functions: Optional[Sequence[Union[DataFunctionLike, str]]] = None,
        providers: Optional[Sequence[ExternalProvider]] = None,
        tests: Optional[Sequence[TestCase]] = None,
    ):

        self.name = name
        if py_module_path:
            py_module_path = os.path.dirname(py_module_path)
        self.py_module_path = py_module_path
        self.py_module_name = py_module_name
        self.library = ComponentLibrary(default_module=self)
        for otype in otypes or []:
            self.add_otype(otype)
        for fn in functions or []:
            self.add_function(fn)
        for p in providers or []:
            self.add_provider(p)
        self.test_cases = tests or []
        # for t in tests or []:
        #     self.add_test_case(t)

    # TODO: implement dir for usability
    # def __dir__(self):
    #     return list(self.members().keys())

    def read_module_file(self, fp: str) -> str:
        if not self.py_module_path:
            raise Exception(f"Module path not set, cannot read {fp}")
        typedef_path = os.path.join(self.py_module_path, fp)
        with open(typedef_path) as f:
            s = f.read()
        return s

    def get_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            return otype_like
        return self.library.get_otype(otype_like)

    def get_function(self, df_like: DataFunctionLike) -> DataFunction:
        from basis.core.data_function import DataFunction

        if isinstance(df_like, DataFunction):
            return df_like
        return self.library.get_function(df_like)

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
    def functions(self) -> ComponentView[ObjectType]:
        return self.library.component_view(ctype=ComponentType.DataFunction)

    @property
    def external_resources(self) -> ComponentView[ObjectType]:
        return self.library.component_view(ctype=ComponentType.External)

    def add_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        otype = self.process_otype(otype_like)
        self.library.add_component(otype)
        return otype

    def process_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            otype = otype_like
        elif isinstance(otype_like, str):
            yml = self.read_module_file(otype_like)
            otype = otype_from_yaml(yml, module_name=self.name)
        else:
            raise TypeError(otype_like)
        return otype

    def add_function(self, df_like: Union[DataFunctionLike, str]) -> DataFunction:
        fn = self.process_function(df_like)
        self.library.add_component(fn)
        return fn

    def process_function(self, df_like: Union[DataFunctionLike, str]) -> DataFunction:
        from basis.core.data_function import (
            DataFunctionDefinition,
            ensure_datafunction_definition,
            DataFunction,
        )
        from basis.core.sql.data_function import sql_datafunction

        if isinstance(df_like, DataFunction):
            df = df_like
        else:
            dfd = None
            if isinstance(df_like, DataFunctionDefinition):
                dfd = df_like
            elif callable(df_like):
                dfd = ensure_datafunction_definition(df_like, module_name=self.name)
            elif isinstance(df_like, str) and df_like.endswith(".sql"):
                if not self.py_module_path:
                    raise Exception(
                        f"Module path not set, cannot read sql definition {df_like}"
                    )
                sql_file_path = os.path.join(self.py_module_path, df_like)
                with open(sql_file_path) as f:
                    sql = f.read()
                file_name = os.path.basename(df_like)[:-4]
                dfd = sql_datafunction(
                    name=file_name, sql=sql, module_name=self.name
                )  # TODO: versions, runtimes, etc for sql (someway to specify in a .sql file)
            else:
                raise Exception(f"Invalid DataFunction {df_like}")
            df = dfd.as_data_function()
        return df

    def add_provider(self, provider: ExternalProvider) -> ExternalProvider:
        p = self.process_provider(provider)
        # self.library.add_component(p)  # TODO
        for r in p.resources:
            self.library.add_component(r)
        return p

    def process_provider(self, provider: ExternalProvider) -> ExternalProvider:
        return provider

    def get_test_env(self) -> Environment:
        # TODO: need way more hooks here (adding runtimes and storages, for instance)
        from basis.core.environment import Environment
        from basis.db.api import create_db, drop_db

        # TODO: what is this hack
        db_name = f"__test_{self.name}"
        try:
            drop_db(f"postgres://postgres@localhost:5432/postgres", db_name)
        except:
            pass
        create_db(f"postgres://postgres@localhost:5432/postgres", db_name)
        pg_url = f"postgres://postgres@localhost:5432/{db_name}"
        env = Environment(f"test_{self.name}", metadata_storage=pg_url,)
        env.add_storage(pg_url)
        env.add_module(self)
        return env

    def run_test(self, test: TestCase):
        # TODO: clean this function up
        env = self.get_test_env()
        fn = env.get_function(test.function)
        dfi = fn.get_interface()
        for case in test.tests:
            print(f"Case {case.name}", end="")
            print(case.test_data_otypes)
            try:
                inputs = {}
                for input in dfi.inputs:
                    test_df = case.test_data[input.name]
                    test_otype = case.test_data_otypes[input.name]
                    n = env.add_external_source_node(
                        f"_test_source_node_{input.name}",
                        "DataFrameExternalResource",
                        config={"dataframe": test_df, "otype": test_otype},
                    )
                    inputs[input.name] = n
                n = env.add_node("_test_node", fn, upstream=inputs)
                output = env.produce(n)
                output_df = output.as_dataframe()
                expected_df = case.test_data["output"]
                expected_otype = env.get_otype(case.test_data_otypes["output"])
                print("Output", id(output_df), output_df)
                print("Expected", expected_df)
                if "output" in case.test_data:
                    assert_dataframes_are_almost_equal(
                        output_df, expected_df, expected_otype
                    )
                print(cf.success("Ok"))
            except Exception as e:
                print(cf.error("Fail:"), str(e))
                raise e

    def run_tests(self):
        print(f"Running tests for module {self.name}")
        for test in self.test_cases:
            print(f"Function {test.function}")
            try:
                self.run_test(test)
            except Exception as e:
                print(e)
                raise e

    # def add_test_case(self, test_case_like: DataFunctionTestCaseLike):
    #     test_case = self.process_test_case(test_case_like)
    #     self.test_cases.extend(test_case)
    #
    # def process_test_case(
    #     self, test_case_like: DataFunctionTestCaseLike
    # ) -> List[DataFunctionTestCase]:
    #     from basis.testing.functions import (
    #         DataFunctionTestCaseLike,
    #         DataFunctionTestCase,
    #         test_cases_from_yaml,
    #     )
    #
    #     if isinstance(test_case_like, DataFunctionTestCase):
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
    #     dfi = self.data_function_indexer()
    #     for otype in self.otypes.all():
    #         for ic in dti.get_indexable_components(otype, self):
    #             yield ic
    #     for s in self.providers.all():
    #         for ic in si.get_indexable_components(s, self):
    #             yield ic
    #     for df in self.data_functions.all():
    #         for ic in dfi.get_indexable_components(df, self):
    #             yield ic


DEFAULT_LOCAL_MODULE = BasisModule("_local_")
