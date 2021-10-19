from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

import typing
from collections import OrderedDict

from pydantic.fields import Field
from basis.configuration.base import FrozenPydanticBase
from basis.configuration.node import NodeCfg


class BasisCfg(FrozenPydanticBase):
    version: str = None
    # TODO other settings (retention policy, error handling, logging, etc)
    # initialize_metadata_storage: bool = True
    # abort_on_function_error: bool = False
    # execution_timelimit_seconds: Optional[int] = None
    # fail_on_downcast: bool = False
    # warn_on_downcast: bool = True
    # use_global_library: bool = True


class GraphCfg(FrozenPydanticBase):
    name: str
    # storages: List[StorageCfg] = []
    # default_storage: Optional[str] = None
    basis: BasisCfg = BasisCfg()
    nodes: List[NodeCfg] = []
    interface: Optional[GraphInterfaceCfg] = None


class GraphInterfaceCfg(FrozenPydanticBase):
    inputs: List[GraphInputCfg] = []
    outputs: typing.OrderedDict[str, str] = Field(default_factory=OrderedDict)
    parameters: Dict[str, Any] = {}


class GraphInputCfg(FrozenPydanticBase):
    name: str
    like: Optional[str] = None
    # TODO: other input settings
    # mode: str
    # schema: str


# Don't think we need this
# class GraphOutputCfg(FrozenPydanticBase):
#     name: str
