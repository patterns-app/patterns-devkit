from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Iterable, Optional, Sequence, Type, Union

from basis.core.typing.object_type import ObjectType, ObjectTypeLike
from basis.utils.registry import Registry, UriRegistry
from basis.utils.uri import UriMixin

if TYPE_CHECKING:
    from basis.core.data_function import (
        ensure_datafunction,
        DataFunctionLike,
        DataFunction,
    )
    from basis.core.external import ExternalProvider
    from basis.indexing.components import (
        IndexableComponent,
        ObjectTypeIndexer,
        ExternalProviderIndexer,
        DataFunctionIndexer,
    )


class BasisModule:
    key: str
    otypes: UriRegistry["ObjectType"]
    data_functions: Registry["DataFunction"]
    module_path: Optional[str]
    module_name: Optional[str]
    providers: Registry["ExternalProvider"]
    provider_indexer: Type["ExternalProviderIndexer"]
    otype_indexer: Type["ObjectTypeIndexer"]
    data_function_indexer: Type["DataFunctionIndexer"]

    def __init__(
        self,
        key: str,
        module_path: Optional[str] = None,
        module_name: Optional[str] = None,
        otypes: Optional[Sequence[ObjectTypeLike]] = None,
        data_functions: Optional[Sequence[Union[DataFunctionLike, str]]] = None,
        providers: Optional[Sequence[ExternalProvider]] = None,
        provider_indexer: Optional[Type[ExternalProviderIndexer]] = None,
        otype_indexer: Optional[Type[ObjectTypeIndexer]] = None,
        data_function_indexer: Optional[Type[DataFunctionIndexer]] = None,
    ):
        from basis.indexing.components import (
            ObjectTypeIndexer,
            ExternalProviderIndexer,
            DataFunctionIndexer,
        )

        self.key = key
        if module_path:
            module_path = os.path.dirname(module_path)
        self.module_path = module_path
        self.module_name = module_name
        self.otypes = UriRegistry()
        self.data_functions = Registry()
        self.providers = Registry()
        self.otypes.register_all(self.process_otypes(otypes or []))
        self.data_functions.register_all(
            self.process_data_functions(data_functions or [])
        )
        self.providers.register_all(self.process_providers(providers or []))
        self.provider_indexer = provider_indexer or ExternalProviderIndexer
        self.otype_indexer = otype_indexer or ObjectTypeIndexer
        self.data_function_indexer = data_function_indexer or DataFunctionIndexer

    # TODO: implement dir for usability
    # def __dir__(self):
    #     return list(self.members().keys())

    def get_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            return otype_like
        return self.otypes.get(otype_like, module_order=[self.key])

    def export(self):
        if self.module_name is None:
            raise Exception("Cannot export module, no module_name set")
        sys.modules[self.module_name] = self  # type: ignore  # sys.modules wants a modulefinder.Module type and it's not gonna get it

    def validate_component(self, component: UriMixin):
        assert component.module_key == self.key

    def process_otypes(self, otypes: Sequence[ObjectTypeLike]) -> Sequence[ObjectType]:
        # TODO: why is this here? Move into ObjectType.from_yaml(...)?
        from basis.core.typing.object_type import ObjectType
        from basis.core.typing.object_type import otype_from_yaml

        processed = []
        for otype in otypes:
            if isinstance(otype, ObjectType):
                processed.append(otype)
            else:
                if not self.module_path:
                    raise Exception("Module path not set")
                typedef_path = os.path.join(self.module_path, otype)
                with open(typedef_path) as f:
                    yml = f.read()
                otype = otype_from_yaml(yml, module_key=self.key)
                processed.append(otype)
        for otype in processed:
            self.validate_component(otype)
        return processed

    def process_data_functions(
        self, data_functions: Sequence[Union[DataFunctionLike, str]]
    ) -> Sequence[DataFunction]:
        from basis.core.data_function import DataFunction, ensure_datafunction
        from basis.core.sql.data_function import sql_datafunction

        processed = []
        for df in data_functions:
            if isinstance(df, DataFunction):
                processed.append(df)
            elif callable(df):
                df = ensure_datafunction(df, module_key=self.key)
                processed.append(df)
            elif isinstance(df, str) and df.endswith(".sql"):
                if not self.module_path:
                    raise Exception("Module path not set")
                sql_file_path = os.path.join(self.module_path, df)
                with open(sql_file_path) as f:
                    sql = f.read()
                file_name = os.path.basename(df)[:-4]
                df = sql_datafunction(
                    key=file_name, sql=sql, module_key=self.key
                )  # TODO: versions, runtimes, etc for sql (someway to specify in a .sql file)
                processed.append(df)
            else:
                raise Exception("Invalid DataFunction")
        for df in processed:
            self.validate_component(df)
        return processed

    def process_providers(
        self, providers: Sequence[ExternalProvider]
    ) -> Sequence[ExternalProvider]:
        return providers
        # """Ensure otypes"""
        # processed = []
        # for s in providers:
        #     for sr in s.reproviders:
        #         sr.otype = self.get_otype(sr.otype)
        # return processed

    def get_indexable_components(self) -> Iterable[IndexableComponent]:
        dti = self.otype_indexer()
        si = self.provider_indexer()
        dfi = self.data_function_indexer()
        for otype in self.otypes.all():
            for ic in dti.get_indexable_components(otype, self):
                yield ic
        for s in self.providers.all():
            for ic in si.get_indexable_components(s, self):
                yield ic
        for df in self.data_functions.all():
            for ic in dfi.get_indexable_components(df, self):
                yield ic
