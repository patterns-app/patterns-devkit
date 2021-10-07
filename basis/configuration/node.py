from __future__ import annotations

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
)
from basis.configuration.storage import StorageCfg
from basis.configuration.base import FrozenPydanticBase


class NodeOutputCfg(FrozenPydanticBase):
    name: Optional[str] = None
    storage: Optional[Union[str, StorageCfg]] = None
    data_format: Optional[str] = None
    retention_policy: Optional[str] = None  # TODO


class NodeCfg(FrozenPydanticBase):
    name: str
    component: str
    component_params: Dict[str, Any] = {}  # TODO: acceptable param types?
    # node_resources: NodeResourcesCfg = NodeResourcesCfg() # TODO
    node_output: NodeOutputCfg = NodeOutputCfg()
    node_inputs: Union[List[str], Dict[str, str], str] = {}
    node_outputs: Dict[str, NodeOutputCfg] = {}
    schedule: Optional[str] = None
