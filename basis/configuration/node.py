from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.storage import StorageCfg


class NodeOutputCfg(FrozenPydanticBase):
    name: Optional[str] = None
    storage: Optional[Union[str, StorageCfg]] = None
    data_format: Optional[str] = None
    retention_policy: Optional[str] = None  # TODO


class NodeCfg(FrozenPydanticBase):
    name: str
    node: str
    node_inputs: Union[List[str], Dict[str, str], str] = {}
    node_params: Dict[str, Any] = {}  # TODO: acceptable param types?
    # node_resources: NodeResourcesCfg = NodeResourcesCfg() # TODO
    default_output_configuration: NodeOutputCfg = NodeOutputCfg()
    output_configurations: Dict[str, NodeOutputCfg] = {}
    schedule: Optional[str] = None
