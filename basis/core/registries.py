# from __future__ import annotations
#
# import os
# import sys
# from dataclasses import dataclass
# from typing import (
#     TYPE_CHECKING,
#     Any,
#     Iterable,
#     List,
#     Optional,
#     Sequence,
#     Type,
#     Union,
#     cast,
# )
#
# from basis.core.typing.object_type import ObjectType, ObjectTypeLike, otype_from_yaml
# from basis.utils.component import DEFAULT_MODULE_NAME, UriMixin
# from basis.utils.registry import Registry, RegistryError, UriRegistry
# from basis.utils.typing import T
#
# if TYPE_CHECKING:
#     from basis.core.data_function import (
#         ensure_datafunction_definition,
#         DataFunctionLike,
#         DataFunctionDefinition,
#         DataFunction,
#     )
#     from basis.core.external import ExternalProvider
#     from basis.core.module import BasisModule
#     from basis.indexing.components import (
#         IndexableComponent,
#         ObjectTypeIndexer,
#         ExternalProviderIndexer,
#         DataFunctionIndexer,
#     )
#
#
# class ComponentRegistry(UriRegistry):
#     def __init__(
#         self,
#         module: Optional[BasisModule] = None,
#         object_key_attr: str = "uri",
#         error_on_duplicate: bool = True,
#     ):
#         super().__init__(object_key_attr, error_on_duplicate)
#         self._module = module
#         self._module_key = self._module.key if self._module else DEFAULT_MODULE_NAME
#
#     def process_and_register_all(self, objs: Iterable[Any]) -> List[T]:
#         processed = []
#         for obj in objs:
#             o = self.process_and_register(obj)
#             processed.append(o)
#         return processed
#
#     def process_and_register(self, obj: Any) -> T:
#         raise NotImplementedError
#
#     def process(self, obj: Any) -> T:
#         raise NotImplementedError
#
#
# class ObjectTypeRegistry(ComponentRegistry):
#     def process_and_register(self, otype_like: ObjectTypeLike) -> ObjectType:
#         otype = self.process(otype_like)
#         self.register(otype)
#         return otype
#
#     def process(self, otype_like: ObjectTypeLike) -> ObjectType:
#         if isinstance(otype_like, ObjectType):
#             otype = otype_like
#         else:
#             if not self._module or not self._module.module_path:
#                 raise Exception("Module path not set")
#             typedef_path = os.path.join(self._module.module_path, otype_like)
#             with open(typedef_path) as f:
#                 yml = f.read()
#             otype = otype_from_yaml(yml, module_key=self._module.key)
#         return otype
#
#
# class DataFunctionRegistry(ComponentRegistry):
#     def process_and_register(
#         self, df_like: Union[DataFunctionLike, str]
#     ) -> DataFunction:
#         dfs = self.process(df_like)
#         self.register(dfs)
#         return dfs
#
#     def process(self, df_like: Union[DataFunctionLike, str]) -> DataFunction:
#         from basis.core.data_function import (
#             DataFunctionDefinition,
#             ensure_datafunction_definition,
#             DataFunction,
#         )
#         from basis.core.sql.data_function import sql_datafunction
#
#         if isinstance(df_like, DataFunction):
#             df = df_like
#         else:
#             dfd = None
#             if isinstance(df_like, DataFunctionDefinition):
#                 dfd = df_like
#             elif callable(df_like):
#                 dfd = ensure_datafunction_definition(
#                     df_like, module_key=self._module_key
#                 )
#             elif isinstance(df_like, str) and df_like.endswith(".sql"):
#                 if not self._module or not self._module.module_path:
#                     raise Exception("Module path not set")
#                 sql_file_path = os.path.join(self._module.module_path, df_like)
#                 with open(sql_file_path) as f:
#                     sql = f.read()
#                 file_name = os.path.basename(df_like)[:-4]
#                 dfd = sql_datafunction(
#                     key=file_name, sql=sql, module_key=self._module_key
#                 )  # TODO: versions, runtimes, etc for sql (someway to specify in a .sql file)
#             else:
#                 raise Exception(f"Invalid DataFunction {df_like}")
#             if self._module:
#                 dfd.module_key = self._module_key
#             df = dfd.as_data_function()
#         if self._module:
#             df.module_key = self._module_key
#         # assert (
#         #     df.module_key == self._module_key
#         # ), f"{df.module_key} {self._module_key}"
#         return df
#
#     def register(self, obj: DataFunction):
#         uri = self.get_key(obj)
#         if uri in self._registry:
#             self.get(uri).merge(obj)
#         else:
#             self._registry[uri] = obj
#             try:
#                 module, key = uri.split(".")
#             except ValueError:
#                 raise RegistryError(f"Invalid URI '{uri}'")
#             self.register_module_key(key, module)
#         # TODO: this feels a bit hidden / hacky ... do we want to register children? Let user do it themselves
#         # TODO: possible inf recursion if sub_funcs include parent (we should ensure they don't...)
#         # for rtc, dfd in obj.runtime_data_functions.items():
#         #     if dfd.sub_functions:
#         #         for sub in dfd.sub_functions:
#         #             if isinstance(sub, str):
#         #                 # Don't register strings
#         #             self.process_and_register(sub)
