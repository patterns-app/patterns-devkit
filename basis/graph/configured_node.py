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

    def find_node(self, path: str, root_path: str = None) -> ConfiguredNode:
        """
        Note: This logic not really necessary now that we compute node_paths
        when building, but this is more efficient search anyways.
        """
        path_parts = path.split(".")
        current_name = path_parts[0]
        remaining_path = ".".join(path_parts[1:])
        found_node = None
        for n in self.nodes:
            if current_name == n.name:
                if remaining_path:
                    assert n.node_type == NodeType.GRAPH
                    found_node = n.find_node(remaining_path, path)
                else:
                    found_node = n
                break
        if found_node is None:
            raise KeyError(f"Node path not found: {path}")
        assert found_node.node_path == (root_path or path)
        return found_node


ConfiguredNode.update_forward_refs()
