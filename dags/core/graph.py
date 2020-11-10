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

from dags.core.node import Node, NodeLike, create_node, inputs_as_nodes
from dags.core.pipe import PipeLike
from dags.utils.common import remove_dupes
from loguru import logger

if TYPE_CHECKING:
    from dags import Environment


class NodeDoesNotExist(KeyError):
    pass


class Graph:
    def __init__(self, env: Environment, declared_nodes: Iterable[Node] = None):
        self.env = env
        self._declared_graph = NodeGraph(declared_nodes)
        self._declared_graph_with_dataset_nodes: Optional[NodeGraph] = None
        self._flattened_graph: Optional[NodeGraph] = None
        # self._compiled_input_nodes: Optional[
        #     Dict[str, Dict[str, NodeLike]]
        # ] = None

    # def __str__(self):
    #     s = "Nodes:\n------\n" + "\n".join(self._nodes.keys())
    #     return s

    def add_node(self, key: str, pipe: Union[PipeLike, str], **kwargs: Any) -> Node:
        from dags.core.node import Node

        if isinstance(pipe, str):
            pipe = self.env.get_pipe(pipe)
        node = create_node(self, key, pipe, **kwargs)
        self._declared_graph.add_node(node)
        self.invalidate_computed_graphs()
        return node

    def declared_nodes(self) -> Iterable[Node]:
        return self._declared_graph.nodes()

    def invalidate_computed_graphs(self):
        self._declared_graph_with_dataset_nodes = None
        self._flattened_graph = None
        # self._compiled_input_nodes = None

    def get_declared_node(self, key: NodeLike) -> Node:
        if isinstance(key, Node):
            return key
        assert isinstance(key, str)
        return self._declared_graph.get_node(key)

    def get_any_node(self, key: NodeLike) -> Node:
        if isinstance(key, Node):
            return key
        assert isinstance(key, str)
        try:
            return self._declared_graph.get_node(key)
        except KeyError:
            pass
        try:
            return self.get_declared_graph_with_dataset_nodes().get_node(key)
        except KeyError:
            pass
        return self.get_flattened_graph().get_node(key)

    def get_declared_graph_with_dataset_nodes(self) -> NodeGraph:
        if self._declared_graph_with_dataset_nodes is not None:
            return self._declared_graph_with_dataset_nodes
        self._declared_graph_with_dataset_nodes = (
            self._declared_graph.with_dataset_nodes()
        )
        return self._declared_graph_with_dataset_nodes

    def get_flattened_graph(self) -> NodeGraph:
        if self._flattened_graph is not None:
            return self._flattened_graph
        self._flattened_graph = self.get_declared_graph_with_dataset_nodes().flatten()
        return self._flattened_graph

    def validate_graph(self) -> bool:
        # TODO
        #   validate node keys are valid
        #   validate pipes are valid
        #   validate types are valid
        #   etc
        pass


class NodeGraph:
    def __init__(
        self,
        nodes: Iterable[Node] = None,
        is_flattened: bool = False,
        compiled_inputs: Dict[str, Dict[str, Node]] = None,
    ):
        self.is_flattened = is_flattened
        self._nodes: Dict[str, Node] = {}
        self._compiled_inputs: Dict[str, Dict[str, Node]] = compiled_inputs or {}
        if nodes:
            for n in nodes:
                self.add_node(n)

    def __str__(self):
        s = "Nodes:\n------\n" + "\n".join(self._nodes.keys())
        return s

    def add_node(self, node: Node):
        if node.key in self._nodes:
            raise KeyError(f"Duplicate node key {node.key}")
        self._nodes[node.key] = node

    def remove_node(self, node: Node):
        del self._nodes[node.key]

    def get_node(self, node_key: str) -> Node:
        return self._nodes[node_key]

    def get_node_like(self, node_like: NodeLike) -> Node:
        if isinstance(node_like, Node):
            return node_like
        return self.get_node(node_like)

    def nodes(self) -> Iterable[Node]:
        return self._nodes.values()

    def copy(self, **kwargs) -> NodeGraph:
        return NodeGraph(
            self._nodes.values(), compiled_inputs=self._compiled_inputs, **kwargs
        )

    def set_compiled_inputs(self, node: Node, inputs: Dict[str, NodeLike]):
        if node.is_composite():
            return self.set_compiled_inputs(node.get_input_node(), inputs)
        self._compiled_inputs[node.key] = {
            name: self.get_node_like(n) for name, n in inputs.items()
        }

    def get_compiled_inputs(self, node: Node) -> Dict[str, Node]:
        return self._compiled_inputs.get(node.key, node.get_declared_input_nodes())

    def get_declared_node_subgraph(self, declared_node_key: str) -> NodeGraph:
        if declared_node_key in self._nodes:
            return NodeGraph([self.get_node(declared_node_key)])
        sub_nodes = []
        for n in self.nodes():
            if (
                n.declared_composite_node_key
                and n.declared_composite_node_key == declared_node_key
            ):
                sub_nodes.append(n)
        return NodeGraph(sub_nodes)

    def declared_input_nodes(self, node: Node) -> Dict[str, Node]:
        raw_inputs = copy(node.get_declared_inputs())
        # inputs = copy(n.get_declared_input_nodes())
        inputs: Dict[str, Node] = {}
        for name, inp in raw_inputs.items():
            if isinstance(inp, str):
                inp = self.get_node(inp)
            elif isinstance(inp, Node):
                pass
            else:
                raise TypeError(inp)
            inputs[name] = inp
        return inputs

    def with_dataset_nodes(self) -> NodeGraph:
        new_g = NodeGraph()
        for n in list(self.nodes()):
            dfi = n.get_interface()
            inputs = self.declared_input_nodes(n)
            for annotation in dfi.inputs:
                if annotation.is_self_ref:
                    # self refs can never be datasets! They operate on the raw block stream
                    # ... i think ...
                    continue
                if annotation.is_dataset:
                    assert (
                        annotation.name is not None
                    )  # inputs should always have a parameter name!
                    input_node = inputs[annotation.name]
                    dsn = input_node.create_dataset_node()
                    try:
                        new_g.add_node(dsn)
                    except KeyError:
                        pass
                    inputs[annotation.name] = dsn
            new_g.set_compiled_inputs(n, inputs)
            try:
                new_g.add_node(n)
            except KeyError:
                # This happens if a Dataset Node is added above but was ALSO declared
                pass
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
            input_names = {
                name: inp.key for name, inp in self.get_compiled_inputs(n).items()
            }
            for input_name, input_node_key in input_names.items():
                if input_node_key == node.key:
                    inputs = self.get_compiled_inputs(n)
                    inputs[input_name] = output_node
                    self.set_compiled_inputs(n, inputs)
                    break
        # Finally, recurse
        for sub_n in node.get_sub_nodes():
            self.flatten_composite_node(sub_n)

    def flatten(self) -> NodeGraph:
        """
        Note, this _modifies_ the existing declared Nodes (by setting their `compiled_inputs` attribute)
        AND creates a new graph with extra / swapped sub-nodes from composite pipes
        """
        new_g = self.copy(is_flattened=True)
        for n in list(new_g.nodes()):
            new_g.flatten_composite_node(n)
        return new_g

    # TODO: rename/factor "compiled"
    def as_networkx_graph(self, compiled: bool) -> nx.DiGraph:
        g = nx.DiGraph()
        for node in self.nodes():
            g.add_node(node.key)
            if compiled:
                inputs = self.get_compiled_inputs(node)
            else:
                inputs = node.get_declared_input_nodes()
            for input_node in inputs.values():
                if input_node.key not in self._nodes:
                    # Don't include nodes not in graph (could be a sub-graph)
                    continue
                g.add_node(input_node.key)
                g.add_edge(input_node.key, node.key)
            # TODO: self ref edge?
        return g

    def get_compiled_networkx_graph(self) -> nx.DiGraph:
        return self.as_networkx_graph(compiled=True)

    def get_declared_networkx_graph(self) -> nx.DiGraph:
        return self.as_networkx_graph(compiled=False)

    # TODO: Think through how to "run" declared composite node (and do we produce datasets too?)
    def get_flattened_output_node_for_declared_node(self, node: Node) -> Node:
        if not self.is_flattened:
            return node
        sub_g = self.get_declared_node_subgraph(node.key)
        node = sub_g.get_all_nodes_in_execution_order()[-1]
        return node

    def get_all_upstream_dependencies_in_execution_order(
        self, node: Node, is_declared_node: bool = True
    ) -> List[Node]:
        g = self.get_compiled_networkx_graph()
        if is_declared_node and self.is_flattened:
            # Translate declared node into root sub-node
            node = self.get_flattened_output_node_for_declared_node(node)
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
        g = self.get_compiled_networkx_graph()
        return [self.get_node(name) for name in nx.topological_sort(g)]
