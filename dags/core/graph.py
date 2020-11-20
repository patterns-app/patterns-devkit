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
        return self.get_declared_graph_with_dataset_nodes().get_node(key)

    def get_declared_graph_with_dataset_nodes(self) -> NodeGraph:
        if self._declared_graph_with_dataset_nodes is not None:
            return self._declared_graph_with_dataset_nodes
        self._declared_graph_with_dataset_nodes = (
            self._declared_graph.with_dataset_nodes()
        )
        return self._declared_graph_with_dataset_nodes

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
        compiled_inputs: Dict[str, Dict[str, Node]] = None,
    ):
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
        self._compiled_inputs[node.key] = {
            name: self.get_node_like(n) for name, n in inputs.items()
        }

    def get_compiled_inputs(self, node: Node) -> Dict[str, Node]:
        return self._compiled_inputs.get(node.key, node.get_declared_input_nodes())

    def declared_input_nodes(self, node: Node) -> Dict[str, Node]:
        raw_inputs = copy(node.get_declared_inputs())
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
            if n.create_dataset:
                ds_nodes = n.create_dataset_nodes()
                try:
                    for dsn in ds_nodes:
                        new_g.add_node(dsn)
                except KeyError:
                    pass
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
                    ds_nodes = input_node.create_dataset_nodes()
                    if ds_nodes:
                        try:
                            for dsn in ds_nodes:
                                new_g.add_node(dsn)
                        except KeyError:
                            pass
                        input_node = ds_nodes[-1]
                    inputs[annotation.name] = input_node
            new_g.set_compiled_inputs(n, inputs)
            try:
                new_g.add_node(n)
            except KeyError:
                # This happens if a Dataset Node is added above but was ALSO declared
                pass
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

    def get_all_upstream_dependencies_in_execution_order(
        self, node: Node
    ) -> List[Node]:
        g = self.get_compiled_networkx_graph()
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
