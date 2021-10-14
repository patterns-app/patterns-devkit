from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration.app import AppCfg
from basis.configuration.base import FrozenPydanticBase
from basis.configuration.storage import StorageCfg


class BasisCfg(FrozenPydanticBase):
    version: str = None
    # TODO other settings (retention policy, error handling, logging, etc)
    # initialize_metadata_storage: bool = True
    # abort_on_function_error: bool = False
    # execution_timelimit_seconds: Optional[int] = None
    # fail_on_downcast: bool = False
    # warn_on_downcast: bool = True
    # use_global_library: bool = True


class DataspaceCfg(FrozenPydanticBase):
    name: str
    storages: List[StorageCfg] = []
    default_storage: Optional[str] = None
    basis: BasisCfg = BasisCfg()
    apps: List[DataspaceAppCfg] = []


class DataspaceAppCfg(FrozenPydanticBase):
    app: str
    name: str
    app_params: Dict[str, Any] = {}
