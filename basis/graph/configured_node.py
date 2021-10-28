from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.graph import (
    NodeDefinitionCfg,
    NodeConnection,
)
from basis.configuration.path import AbsoluteNodeConnection


class NodeType(str, Enum):
    NODE = "node"
    GRAPH = "graph"


class ConfiguredNode(FrozenPydanticBase):
    # Name
    node_name: str
    # Computed graph attrs
    absolute_node_path: str
    node_depth: int
    # Basic attrs
    node_type: NodeType
    description: Optional[str] = None
    absolute_file_path_to_config_yaml: Optional[str] = None
    node_definition: Optional[NodeDefinitionCfg] = None
    # Configuration
    parameter_values: Dict[str, Any] = {}
    output_aliases: Dict[str, str] = {}
    schedule: Optional[str] = None
    labels: Optional[List[str]] = None
    # Graph configuration
    declared_connections: List[NodeConnection] = []
    flattened_connections: List[AbsoluteNodeConnection] = []

    def input_connections(self) -> Iterator[AbsoluteNodeConnection]:
        for abs_conn in self.flattened_connections:
            if abs_conn.output_path.absolute_node_path == self.absolute_node_path:
                yield abs_conn

    def output_connections(self) -> Iterator[AbsoluteNodeConnection]:
        for abs_conn in self.flattened_connections:
            if abs_conn.input_path.absolute_node_path == self.absolute_node_path:
                yield abs_conn


ConfiguredNode.update_forward_refs()


# def find_node(self, path: str, root_path: str = None) -> ConfiguredNode:
#     """
#     Note: This logic not really necessary now that we compute node_paths
#     when building, but this is more efficient search anyways.
#     """
#     path_parts = path.split(".")
#     current_name = path_parts[0]
#     remaining_path = ".".join(path_parts[1:])
#     found_node = None
#     for n in self.nodes:
#         if current_name == n.name:
#             if remaining_path:
#                 assert n.node_type == NodeType.GRAPH
#                 found_node = n.find_node(remaining_path, path)
#             else:
#                 found_node = n
#             break
#     if found_node is None:
#         raise KeyError(f"Node path not found: {path}")
#     assert found_node.node_path == (root_path or path)
#     return found_node