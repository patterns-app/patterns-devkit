from __future__ import annotations

from enum import Enum
from typing import Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from commonmodel import Schema
from networkx.classes.graph import Graph
from basis.core.component import ComponentLibrary, global_library
from basis.core.declarative.base import FrozenPydanticBase
from basis.core.declarative.flow import FlowCfg
from basis.core.declarative.function import DataFunctionCfg
from basis.core.declarative.graph import GraphCfg
from basis.core.declarative.interface import BoundInterfaceCfg

NxNode = Tuple[str, Dict[str, Dict]]
NxAdjacencyList = List[NxNode]


class BasisCfg(FrozenPydanticBase):
    initialize_metadata_storage: bool = True
    abort_on_function_error: bool = False
    execution_timelimit_seconds: Optional[int] = None
    fail_on_downcast: bool = False
    warn_on_downcast: bool = True
    use_global_library: bool = True


class DataspaceCfg(FrozenPydanticBase):
    key: Optional[str] = None
    namespaces: List[str] = []
    # library: Optional[ComponentLibraryCfg] = None
    metadata_storage: Optional[str] = None
    default_storage: Optional[str] = None
    storages: List[str] = []
    basis: BasisCfg = BasisCfg()
    graph: GraphCfg = GraphCfg()

    def resolve(self, lib: ComponentLibrary = None) -> DataspaceCfg:
        if lib is None:
            lib = global_library
        d = self.dict()
        d["graph"] = self.graph.resolve(lib)
        # d["library"] = lib.to_config() # TODO: too big to serialize? And for what reason?
        return DataspaceCfg(**d)


class ComponentLibraryCfg(FrozenPydanticBase):
    functions: List[DataFunctionCfg] = []
    schemas: List[Schema] = []
    flows: List[FlowCfg] = []
    namespace_precedence: List[str] = []
