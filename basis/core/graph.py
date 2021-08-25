from __future__ import annotations
from basis.core.declarative.node import NodeCfg
from dataclasses import dataclass, field
from basis.core.node import Node, instantiate_node

from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import networkx as nx
from basis.core.component import ComponentLibrary, global_library
from basis.core.declarative.base import FrozenPydanticBase, PydanticBase
from basis.core.declarative.function import (
    FunctionCfg,
    FunctionInterfaceCfg,
)
from commonmodel import Schema
from dcp.utils.common import as_identifier, remove_dupes
from loguru import logger
from pydantic import validator
from pydantic.class_validators import root_validator

if TYPE_CHECKING:
    from basis.core.declarative.flow import FlowCfg
    from basis.core.declarative.interface import NodeInputCfg


NxNode = Tuple[str, Dict[str, Dict]]
NxAdjacencyList = List[NxNode]


def startswith_any(s: str, others: Iterable[str]) -> bool:
    for o in others:
        if not o:
            continue
        if s.startswith(o):
            return True
    return False


def ensure_enum_str(s: Union[str, Enum]) -> str:
    if isinstance(s, str):
        return s
    if isinstance(s, Enum):
        return s.value
    raise TypeError(s)


class ImproperlyConfigured(Exception):
    pass


# class DedupeBehavior(str, Enum):
#     NONE = "None"
#     LATEST_RECORD = "LatestRecord"
#     FIRST_NON_NULL_VALUES = "FirstNonNullValues"
#     LATEST_NON_NULL_VALUES = "LatestNonNullValues"


class NodeOutputCfg(FrozenPydanticBase):
    storage: Optional[str] = None
    data_format: Optional[str] = None
    # retention_policy: Optional[str] = None # TODO


@dataclass
class Graph:
    nodes: List[Node] = field(default_factory=list)

    # def check_unique_nodes(cls, nodes: List[Node]) -> List[Node]:
    #     assert len(nodes) == len(set(n.key for n in nodes)), "Node keys must be unique"
    #     return nodes

    def get_all_schema_keys(self) -> List[str]:
        schemas = []
        if self.nodes:
            for n in self.nodes:
                schemas.extend(n.get_all_schema_keys())
        return schemas

    def node_dict(self) -> Dict[str, Node]:
        return {n.key: n for n in self.nodes if n.key}

    def add(self, n: Node):
        if n.key in self.node_dict():
            raise KeyError(
                f"Duplicate node key `{n.key}`. Specify a distinct key for the node/graph"
            )
        self.nodes.append(n)

    def get_node(self, key: Union[Node, str]) -> Node:
        if isinstance(key, Node):
            return key
        assert isinstance(key, str)
        return self.node_dict()[key]

    def get_nodes_with_prefix(self, prefix: Union[Node, str]) -> List[Node]:
        if isinstance(prefix, Node):
            prefix = prefix.key
        return [n for n in self.nodes if n.key and n.key.startswith(prefix)]

    def as_nx_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        for n in self.nodes:
            g.add_node(n.key)
            inputs = n.get_inputs()
            for input_node_key in inputs.values():
                g.add_node(input_node_key)
                g.add_edge(input_node_key, n.key)
        return g

    def adjacency_list(self) -> NxAdjacencyList:
        return list(self.as_nx_graph().adjacency())

    def get_all_upstream_dependencies_in_execution_order(
        self, node: Node
    ) -> List[Node]:
        g = self.as_nx_graph()
        node_keys = self._get_all_upstream_dependencies_in_execution_order(g, node.key)
        return [self.get_node(name) for name in node_keys]

    def _get_all_upstream_dependencies_in_execution_order(
        self, g: nx.DiGraph, node: str
    ) -> List[str]:
        nodes = []
        for parent_node in g.predecessors(node):
            if parent_node == node:
                # Ignore self-ref cycles
                continue
            parent_deps = self._get_all_upstream_dependencies_in_execution_order(
                g, parent_node
            )
            nodes.extend(parent_deps)
        nodes.append(node)
        # May have added nodes twice, just keep first reference:
        return remove_dupes(nodes)

    def get_all_nodes_in_execution_order(self) -> List[Node]:
        g = self.as_nx_graph()
        return [self.get_node(name) for name in nx.topological_sort(g)]

    def get_node_inputs(self, node: Node) -> Dict[str, NodeInputCfg]:
        from basis.core.declarative.interface import NodeInputCfg

        declared_inputs = node.inputs
        node_inputs = {}
        for inpt in node.get_interface().inputs.values():
            declared_input = declared_inputs.get(inpt.name)
            input_node = None
            if declared_input:
                input_node = self.get_node(declared_input)
            #     if input_node is None and inpt.is_self_reference:
            #         input_node = self
            node_inputs[inpt.name] = NodeInputCfg(
                name=inpt.name,
                input=inpt,
                input_node=input_node,
                # schema_translation=self.get_schema_translations().get(
                #     inpt.name
                # ),  # TODO: doesn't handle stdin
            )
        return node_inputs


def instantiate_graph(nodes: List[NodeCfg], lib: ComponentLibrary) -> Graph:
    return Graph(nodes=[instantiate_node(n, lib) for n in nodes])
