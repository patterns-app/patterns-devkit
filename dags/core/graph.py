from __future__ import annotations

from copy import copy
from pprint import pprint
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import networkx as nx

from dags.core.node import Node
from loguru import logger

if TYPE_CHECKING:
    pass


class Graph:
    def __init__(self, nodes: Iterable[Node] = None, is_flattened: bool = False):
        self.is_flattened = is_flattened
        self._nodes: Dict[str, Node] = {}
        if nodes:
            for n in nodes:
                self.add_node(n)

    def add_node(self, node: Node):
        self._nodes[node.name] = node

    def remove_node(self, node: Node):
        del self._nodes[node.name]

    def get_node(self, node_name: str) -> Node:
        return self._nodes[node_name]

    def nodes(self) -> Iterable[Node]:
        return self._nodes.values()

    def copy(self, **kwargs) -> Graph:
        return Graph(self._nodes.values(), **kwargs)

    def get_declared_node_subgraph(self, declared_node_name: str) -> Graph:
        if declared_node_name in self._nodes:
            return Graph([self.get_node(declared_node_name)])
        sub_nodes = []
        for n in self.nodes():
            if (
                n.declared_composite_node_name
                and n.declared_composite_node_name == declared_node_name
            ):
                sub_nodes.append(n)
        return Graph(sub_nodes)

    def add_dataset_nodes(self) -> Graph:
        new_g = Graph()
        for n in list(self.nodes()):
            dfi = n.get_interface()
            for annotation in dfi.inputs:
                if annotation.is_dataset:
                    inputs = copy(n.get_declared_input_nodes())
                    input_node = inputs[annotation.name]
                    dsn = input_node.get_or_create_dataset_node()
                    new_g.add_node(dsn)
                    inputs[annotation.name] = dsn
                    n.set_compiled_inputs(inputs)
            new_g.add_node(n)
        return new_g

    def flatten_composite_node(self, node: Node):
        # TODO: this changes the *nodes'* state, not ideal, need to make sure you are working with copy
        # One option, always a favorite, is to make frozen DC
        if not node.is_composite():
            return
        # Add new child nodes
        for sub_n in node.get_sub_nodes():
            self.add_node(sub_n)
        # Remove composite, we are done with it
        self.remove_node(node)
        # Replace any input references with new output node
        output_node = node.get_output_node()
        for n in self.nodes():
            input_names = n.get_compiled_input_names()
            for input_name, input_node_name in input_names.items():
                if input_node_name == node.name:
                    inputs = n.get_compiled_input_nodes()
                    inputs[input_name] = output_node
                    n.set_compiled_inputs(inputs)
                    break
        # Finally, recurse
        for sub_n in node.get_sub_nodes():
            self.flatten_composite_node(sub_n)

    def flatten(self) -> Graph:
        """
        Note, this _modifies_ the existing declared Nodes (by setting their `compiled_inputs` attribute)
        AND creates a new graph with extra / swapped sub-nodes from composite pipes
        """
        new_g = self.copy(is_flattened=True)
        for n in list(new_g.nodes()):
            new_g.flatten_composite_node(n)
        return new_g

    def as_networkx_graph(self, compiled: bool) -> nx.DiGraph:
        g = nx.DiGraph()
        for node in self.nodes():
            g.add_node(node.name)
            if compiled:
                inputs = node.get_compiled_input_nodes()
            else:
                inputs = node.get_declared_input_nodes()
            for input_node in inputs.values():
                if input_node.name not in self._nodes:
                    # Don't include nodes not in graph (could be a sub-graph)
                    continue
                g.add_node(input_node.name)
                g.add_edge(input_node.name, node.name)
            # TODO: self ref edge?
        return g

    def get_compiled_networkx_graph(self) -> nx.DiGraph:
        return self.as_networkx_graph(compiled=True)

    def get_declared_networkx_graph(self) -> nx.DiGraph:
        return self.as_networkx_graph(compiled=False)

    def get_flattened_root_node_for_declared_node(self, node: Node) -> Node:
        if not self.is_flattened:
            return node
        sub_g = self.get_declared_node_subgraph(node.name)
        node = sub_g.get_all_nodes_in_execution_order()[0]
        return node

    def get_all_upstream_dependencies_in_execution_order(
        self, node: Node, is_declared_node: bool = True
    ) -> List[Node]:
        g = self.get_compiled_networkx_graph()
        if is_declared_node and self.is_flattened:
            # Translate declared node into root sub-node
            node = self.get_flattened_root_node_for_declared_node(node)
        node_names = self._get_all_upstream_dependencies_in_execution_order(
            g, node.name
        )
        return [self.get_node(name) for name in node_names]

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
        return nodes

    def get_all_nodes_in_execution_order(self) -> List[Node]:
        g = self.get_compiled_networkx_graph()
        return [self.get_node(name) for name in nx.topological_sort(g)]
