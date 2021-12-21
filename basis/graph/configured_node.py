from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Iterator, List, Union, Optional

from commonmodel import Schema

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.path import GraphEdge, NodeId

"""The version of schemas generated with this code"""
CURRENT_MANIFEST_SCHEMA_VERSION = 1


class GraphError(FrozenPydanticBase):
    # id of the node that created the error, or None for errors on the root graph yaml
    node_id: Optional[NodeId]
    message: str


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
    Webhook = "webhook"
    Chart = "chart"


class PortType(str, Enum):
    Table = "table"
    Stream = "stream"


class InputDefinition(FrozenPydanticBase):
    port_type: PortType

    # for python files: the name of the node function parameter
    # for sql: the name of the table used
    # for graphs: the exposed port name
    # for charts: the chart_input port name
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


class StateDefinition(FrozenPydanticBase):
    name: str


class NodeInterface(FrozenPydanticBase):
    inputs: List[InputDefinition]
    outputs: List[OutputDefinition]
    parameters: List[ParameterDefinition]
    state: StateDefinition = None


class ConfiguredNode(FrozenPydanticBase):
    name: str
    node_type: NodeType
    id: NodeId
    # declared ports
    interface: NodeInterface
    description: str = None
    parent_node_id: NodeId = None
    file_path_to_node_script_relative_to_root: str = None
    parameter_values: Dict[str, Any]
    schedule: str = None
    # edges as declared in the node, may point to graph nodes, will not point to nodes in sub- or super-graphs.
    local_edges: List[GraphEdge]
    # resolved edges will only point to execution nodes, may point to nodes in sub- or super-graphs.
    resolved_edges: List[GraphEdge]

    def local_input_edges(self) -> Iterator[GraphEdge]:
        for e in self.local_edges:
            if e.output.node_id == self.id:
                yield e

    def local_output_edges(self) -> Iterator[GraphEdge]:
        for e in self.local_edges:
            if e.input.node_id == self.id:
                yield e

    def resolved_input_edges(self) -> Iterator[GraphEdge]:
        for e in self.resolved_edges:
            if e.output.node_id == self.id:
                yield e

    def resolved_output_edges(self) -> Iterator[GraphEdge]:
        for e in self.resolved_edges:
            if e.input.node_id == self.id:
                yield e


class GraphManifest(FrozenPydanticBase):
    graph_name: str
    manifest_version: int
    nodes: List[ConfiguredNode] = []
    errors: List[GraphError] = []

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
        assert (
            len(nodes) == 1
        ), f"Must be exactly one node of name `{name}`, found {len(nodes)}"
        return nodes[0]

    def get_errors_for_node(
        self, node_or_id: Union[str, NodeId, ConfiguredNode]
    ) -> Iterator[GraphError]:
        id = node_or_id.id if isinstance(node_or_id, ConfiguredNode) else node_or_id
        for error in self.errors:
            if error.node_id == id:
                yield error
