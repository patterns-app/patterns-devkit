from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Iterator, List, Union

from commonmodel import Schema

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.path import AbsoluteEdge, DeclaredEdge, NodeId

"""The version of schemas generated with this code"""
CURRENT_MANIFEST_SCHEMA_VERSION = 1


class ParameterType(str, Enum):
    Text = "text"
    Boolean = "bool"
    Integer = "int"
    Float = "float"
    Date = "date"
    DateTime = "datetime"


class NodeType(str, Enum):
    Node = "node"
    Graph = "graph"


class PortType(str, Enum):
    Table = "table"
    Stream = "stream"


class InputDefinition(FrozenPydanticBase):
    port_type: PortType

    # for python files: the name of the node function parameter
    # for sql: the name of the table used
    # for graphs: the exposed port name
    name: str

    description: str = None
    schema_or_name: Union[str, Schema] = None
    required: bool


class OutputDefinition(FrozenPydanticBase):
    port_type: PortType
    name: str
    description: str = None
    schema_or_name: Union[str, Schema] = None


class ParameterDefinition(FrozenPydanticBase):
    name: str
    parameter_type: ParameterType = None
    description: str = None
    default: Any = None


class NodeInterface(FrozenPydanticBase):
    inputs: List[InputDefinition]
    outputs: List[OutputDefinition]
    parameters: List[ParameterDefinition]


class ConfiguredNode(FrozenPydanticBase):
    name: str
    node_type: NodeType
    id: NodeId
    # declared ports
    interface: NodeInterface
    # distance from root graph, starts at 0
    node_depth: int
    description: str = None
    parent_node_id: NodeId = None
    file_path_to_node_script_relative_to_root: str = None
    # Configuration
    parameter_values: Dict[str, Any]
    schedule: str = None
    # Graph configuration
    declared_edges: List[DeclaredEdge]
    absolute_edges: List[AbsoluteEdge]

    def input_edges(self) -> Iterator[AbsoluteEdge]:
        for e in self.absolute_edges:
            if e.output_path.node_id == self.id:
                yield e

    def output_edges(self) -> Iterator[AbsoluteEdge]:
        for e in self.absolute_edges:
            if e.input_path.node_id == self.id:
                yield e


class GraphManifest(FrozenPydanticBase):
    graph_name: str
    manifest_version: int
    nodes: List[ConfiguredNode] = []

    def get_node_by_id(self, node_id: Union[str, NodeId]) -> ConfiguredNode:
        for n in self.nodes:
            if n.id == node_id:
                return n
        raise KeyError(node_id)

    def get_nodes_by_name(self, name: str) -> Iterator[ConfiguredNode]:
        for node in self.nodes:
            if node.name == name:
                yield node

    def get_single_node_by_name(self, name: str) -> ConfiguredNode:
        nodes = list(self.get_nodes_by_name(name))
        assert len(nodes) == 1
        return nodes[0]
