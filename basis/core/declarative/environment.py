from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.core.declarative.base import FrozenPydanticBase
from basis.core.declarative.flow import FlowCfg
from basis.core.declarative.function import FunctionCfg, FunctionSourceFileCfg
from commonmodel import Schema

NxNode = Tuple[str, Dict[str, Dict]]
NxAdjacencyList = List[NxNode]


class BasisCfg(FrozenPydanticBase):
    initialize_metadata_storage: bool = True
    abort_on_function_error: bool = False
    execution_timelimit_seconds: Optional[int] = None
    fail_on_downcast: bool = False
    warn_on_downcast: bool = True
    use_global_library: bool = True


class EnvironmentCfg(FrozenPydanticBase):
    key: Optional[str] = None
    # library: Optional[ComponentLibraryCfg] = None
    modules: List[str] = []
    metadata_storage: Optional[str] = None
    default_storage: Optional[str] = None
    default_runtime: Optional[str] = None
    storages: List[str] = []
    runtimes: List[str] = []
    basis_cfg: BasisCfg = BasisCfg()


class ComponentLibraryCfg(FrozenPydanticBase):
    functions: List[FunctionCfg] = []
    schemas: List[Schema] = []
    flows: List[FlowCfg] = []
    source_file_functions: List[FunctionSourceFileCfg] = []
    namespace_precedence: List[str] = []
