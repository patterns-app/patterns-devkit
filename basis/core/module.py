from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Iterable, Optional, Sequence, Type, Union

from basis.core.component import (
    ComponentLibrary,
    ComponentType,
    ComponentUri,
    ComponentView,
)
from basis.core.typing.object_type import ObjectType, ObjectTypeLike, otype_from_yaml

if TYPE_CHECKING:
    from basis.core.data_function import (
        DataFunctionLike,
        DataFunction,
    )
    from basis.core.external import ExternalProvider


class BasisModule:
    name: str
    py_module_path: Optional[str]
    py_module_name: Optional[str]
    library: ComponentLibrary

    def __init__(
        self,
        name: str,
        py_module_path: Optional[str] = None,
        py_module_name: Optional[str] = None,
        otypes: Optional[Sequence[ObjectTypeLike]] = None,
        functions: Optional[Sequence[Union[DataFunctionLike, str]]] = None,
        providers: Optional[Sequence[ExternalProvider]] = None,
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

    # TODO: implement dir for usability
    # def __dir__(self):
    #     return list(self.members().keys())

    def get_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            return otype_like
        return self.library.get_otype(otype_like)

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

    def add_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        otype = self.process_otype(otype_like)
        self.library.add_component(otype)
        return otype

    def process_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            otype = otype_like
        elif isinstance(otype_like, str):
            if not self.py_module_path:
                raise Exception(
                    f"Module path not set, cannot read otype definition {otype_like}"
                )
            typedef_path = os.path.join(self.py_module_path, otype_like)
            with open(typedef_path) as f:
                yml = f.read()
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
        self.library.add_component(p)
        return p

    def process_provider(self, provider: ExternalProvider) -> ExternalProvider:
        return provider

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
