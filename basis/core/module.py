from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Iterable, Optional, Sequence, Type, Union

from basis.core.object_type import ObjectType, ObjectTypeLike
from basis.utils.registry import Registry, UriRegistry

if TYPE_CHECKING:
    from basis.core.data_function import (
        ensure_datafunction,
        DataFunctionLike,
        DataFunction,
    )
    from basis.core.source_resource import Source
    from basis.indexing.components import (
        IndexableComponent,
        ObjectTypeIndexer,
        SourceIndexer,
        DataFunctionIndexer,
    )


class BasisModule:
    key: str
    otypes: UriRegistry["ObjectType"]
    data_functions: Registry["DataFunction"]
    module_path: Optional[str]
    module_name: Optional[str]
    sources: Registry["Source"]
    source_indexer: Type["SourceIndexer"]
    otype_indexer: Type["ObjectTypeIndexer"]
    data_function_indexer: Type["DataFunctionIndexer"]

    def __init__(
        self,
        key: str,
        module_path: Optional[str] = None,
        module_name: Optional[str] = None,
        otypes: Optional[Sequence[ObjectTypeLike]] = None,
        data_functions: Optional[Sequence[Union[DataFunctionLike, str]]] = None,
        sources: Optional[Sequence[Source]] = None,
        source_indexer: Optional[Type[SourceIndexer]] = None,
        otype_indexer: Optional[Type[ObjectTypeIndexer]] = None,
        data_function_indexer: Optional[Type[DataFunctionIndexer]] = None,
    ):
        from basis.indexing.components import (
            ObjectTypeIndexer,
            SourceIndexer,
            DataFunctionIndexer,
        )

        self.key = key
        if module_path:
            module_path = os.path.dirname(module_path)
        self.module_path = module_path
        self.module_name = module_name
        self.otypes = UriRegistry()
        self.data_functions = Registry()
        self.sources = Registry()
        self.otypes.register_all(self.process_otypes(otypes or []))
        self.data_functions.register_all(
            self.process_data_functions(data_functions or [])
        )
        self.sources.register_all(self.process_sources(sources or []))
        self.source_indexer = source_indexer or SourceIndexer
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

    def process_otypes(self, otypes: Sequence[ObjectTypeLike]) -> Sequence[ObjectType]:
        # TODO: why is this here? Move into ObjectType.from_yaml(...)?
        from basis.core.object_type import ObjectType
        from basis.core.object_type import otype_from_yaml

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
            assert otype.module_key == self.key
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
                df = ensure_datafunction(df)
                processed.append(df)
            elif isinstance(df, str) and df.endswith(".sql"):
                if not self.module_path:
                    raise Exception("Module path not set")
                sql_file_path = os.path.join(self.module_path, df)
                with open(sql_file_path) as f:
                    sql = f.read()
                df = sql_datafunction(sql, key=df[:-4])
                processed.append(df)
            else:
                raise Exception("Invalid DataFunction")
        return processed

    def process_sources(self, sources: Sequence[Source]) -> Sequence[Source]:
        return sources
        # """Ensure otypes"""
        # processed = []
        # for s in sources:
        #     for sr in s.resources:
        #         sr.otype = self.get_otype(sr.otype)
        # return processed

    def get_indexable_components(self) -> Iterable[IndexableComponent]:
        dti = self.otype_indexer()
        si = self.source_indexer()
        dfi = self.data_function_indexer()
        for otype in self.otypes.all():
            for ic in dti.get_indexable_components(otype, self):
                yield ic
        for s in self.sources.all():
            for ic in si.get_indexable_components(s, self):
                yield ic
        for df in self.data_functions.all():
            for ic in dfi.get_indexable_components(df, self):
                yield ic
