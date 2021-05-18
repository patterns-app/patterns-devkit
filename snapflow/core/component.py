from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, List, Union

from commonmodel import Schema

if TYPE_CHECKING:
    from snapflow.core.function import (
        DataFunctionLike,
        DataFunction,
    )
    from snapflow.core.module import SnapflowModule
    from snapflow.core.declarative.flow import FlowCfg


DEFAULT_LOCAL_NAMESPACE = "_local"
DEFAULT_NAMESPACE = DEFAULT_LOCAL_NAMESPACE


class DictView(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


class ComponentLibrary:
    functions: Dict[str, DataFunction]
    schemas: Dict[str, Schema]
    flows: Dict[str, FlowCfg]
    namespace_precedence: List[str]

    def __init__(self, namespace_precedence: List[str] = None):
        self.functions = {}
        self.schemas = {}
        self.flows = {}
        self.namespace_precedence = [DEFAULT_LOCAL_NAMESPACE]
        if namespace_precedence:
            for k in namespace_precedence:
                self.add_namespace(k)

    def add_namespace(self, k: str):
        k = k.split(".")[0]
        if k not in self.namespace_precedence:
            self.namespace_precedence.append(k)

    def add_module(self, module: Union[SnapflowModule, ModuleType]):
        if isinstance(module, ModuleType):
            # TODO: hack?
            module = module.module
        self.merge(module.library)

    def add_function(self, f: DataFunction):
        self.add_namespace(f.key)
        self.functions[f.key] = f

    def add_schema(self, schema: Schema):
        self.add_namespace(schema.key)
        self.schemas[schema.key] = schema

    def add_flow(self, f: FlowCfg):
        self.add_namespace(f.key)
        self.flows[f.key] = f

    def remove_function(self, function_like: Union[DataFunction, str]):
        from snapflow.core.function import DataFunction

        if isinstance(function_like, DataFunction):
            function_like = function_like.key
        if function_like not in self.functions:
            return
        del self.functions[function_like]

    def get_function(
        self, function_like: Union[DataFunction, str], try_module_lookups=True
    ) -> DataFunction:
        from snapflow.core.function import DataFunction

        if isinstance(function_like, DataFunction):
            return function_like
        if not isinstance(function_like, str):
            raise TypeError(function_like)
        try:
            return self.functions[function_like]
        except KeyError as e:
            if try_module_lookups:
                return self.namespace_lookup(self.functions, function_like)
            raise e

    def get_schema(
        self, schema_like: Union[Schema, str], try_module_lookups=True
    ) -> Schema:

        if isinstance(schema_like, Schema):
            return schema_like
        if not isinstance(schema_like, str):
            raise TypeError(schema_like)
        try:
            return self.schemas[schema_like]
        except KeyError as e:
            if try_module_lookups:
                return self.namespace_lookup(self.schemas, schema_like)
            raise e

    def get_flow(
        self, flow_like: Union[FlowCfg, str], try_module_lookups=True
    ) -> FlowCfg:
        from snapflow.core.declarative.flow import FlowCfg

        if isinstance(flow_like, FlowCfg):
            return flow_like
        if not isinstance(flow_like, str):
            raise TypeError(flow_like)
        try:
            return self.flows[flow_like]
        except KeyError as e:
            if try_module_lookups:
                return self.namespace_lookup(self.flows, flow_like)
            raise e

    def namespace_lookup(self, d: Dict[str, Any], k: str) -> Any:
        if "." in k:
            raise KeyError(k)
        for m in self.namespace_precedence:
            try:
                return d[m + "." + k]
            except KeyError:
                pass
        raise KeyError(f"`{k}` not found in modules {self.namespace_precedence}")

    def all_functions(self) -> List[DataFunction]:
        return list(self.functions.values())

    def all_schemas(self) -> List[Schema]:
        return list(self.schemas.values())

    def merge(self, other: ComponentLibrary):
        self.functions.update(other.functions)
        self.schemas.update(other.schemas)
        self.flows.update(other.flows)
        for k in other.namespace_precedence:
            self.add_namespace(k)

    def get_view(self, d: Dict) -> DictView[str, Any]:
        ad: DictView = DictView()
        for k, p in d.items():
            # ad[k] = p
            ad[k.split(".")[-1]] = p  # TODO: module precedence
        return ad

    def get_functions_view(self) -> DictView[str, DataFunction]:
        return self.get_view(self.functions)

    def get_schemas_view(self) -> DictView[str, Schema]:
        return self.get_view(self.schemas)


global_library = ComponentLibrary()
