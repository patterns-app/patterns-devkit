from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union
from basis.configuration.app import AppCfg
from basis.configuration.storage import StorageCfg
from basis.configuration.base import FrozenPydanticBase


class BasisCfg(FrozenPydanticBase):
    version: str = None
    # TODO which of these are needed still
    # initialize_metadata_storage: bool = True
    # abort_on_function_error: bool = False
    # execution_timelimit_seconds: Optional[int] = None
    # fail_on_downcast: bool = False
    # warn_on_downcast: bool = True
    # use_global_library: bool = True


class ProjectCfg(FrozenPydanticBase):
    name: str
    storages: List[StorageCfg] = []
    default_storage: Optional[str] = None
    basis: BasisCfg = BasisCfg()
    graph: List[AppNodeCfg] = []
    # library: Optional[ComponentLibraryCfg] = None
    # metadata_storage: Optional[str] = None
    # default_runtime: Optional[str] = None
    # runtimes: List[str] = []


class AppNodeCfg(FrozenPydanticBase):
    app: str
    name: str
    app_params: Dict[str, Any] = {}


# class ComponentLibraryCfg(FrozenPydanticBase):
#     # functions: List[FunctionCfg] = []
#     schemas: List[Schema] = []
#     # flows: List[FlowCfg] = []
#     source_file_functions: List[FunctionSourceFileCfg] = []
#     namespace_precedence: List[str] = []


# class ProjectCfg(EnvironmentCfg):
#     nodes: List[NodeCfg]

#     def as_environment_cfg(self) -> EnvironmentCfg:
#         d = self.dict()
#         del d["nodes"]
#         return EnvironmentCfg(**d)
