from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.graph import GraphCfg
from basis.configuration.node import GraphNodeCfg, NodeType
from basis.node.interface import NodeInterface


class ConfiguredNode(FrozenPydanticBase):
    name: str
    node_path: str
    node_type: NodeType
    interface: NodeInterface
    readme: Optional[str] = None
    path_from_graph_root_to_node_file: Optional[str] = None
    python_path_from_graph_root_to_node_object: Optional[str] = None
    nodes: List[ConfiguredNode] = []
    inputs: Dict[str, str] = {}
    parameters: Dict[str, Any] = {}
    original_cfg: Union[GraphCfg, GraphNodeCfg, None] = None


ConfiguredNode.update_forward_refs()
