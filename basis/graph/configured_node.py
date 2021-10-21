from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.graph import GraphCfg
from basis.configuration.node import GraphNodeCfg
from basis.node.interface import NodeInterface


class ConfiguredNode(FrozenPydanticBase):
    name: str
    interface: NodeInterface
    nodes: List[ConfiguredNode] = []
    inputs: Dict[str, str] = {}
    parameters: Dict[str, Any] = {}
    original_cfg: Optional[Union[GraphCfg, GraphNodeCfg]] = None


ConfiguredNode.update_forward_refs()
