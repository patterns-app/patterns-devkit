from __future__ import annotations

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

from basis.core.node import Node
from loguru import logger

if TYPE_CHECKING:
    pass


class Graph:
    def __init__(self, nodes: List[Node] = None):
        self._nodes: Dict[str, Node] = {}
        if nodes:
            for n in nodes:
                self.add_node(n)

    def add_node(self, node: Node):
        self._nodes[node.name] = node

    def get_node(self, node_name: str) -> Node:
        return self._nodes[node_name]

    def nodes(self) -> Iterable[Node]:
        return self._nodes.values()

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
        for n in self.nodes():
            new_n = n.clone()
            dfi = new_n.get_interface()
            for annotation in dfi.inputs:
                if annotation.is_dataset:
                    dsn = new_n.get_dataset_node()
                    new_g.add_node(dsn)
                    new_n.get_raw_inputs()[annotation.name] = dsn
            new_g.add_node(new_n)
        return new_g

    def flatten(self) -> Graph:
        new_g = Graph()
        for n in self.nodes():
            if n.data_function.is_composite:
                for sub_n in n.make_sub_nodes():
                    new_g.add_node(sub_n)
            else:
                new_g.add_node(n)
        return new_g

    def as_networkx_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        for node in self.nodes():
            g.add_node(node.name)
            for raw_input in node.get_raw_inputs().values():
                if isinstance(raw_input, Node):
                    raw_input = raw_input.name
                if raw_input not in self._nodes:
                    # Don't include nodes not in graph (could be a sub-graph)
                    continue
                g.add_node(raw_input)
                g.add_edge(raw_input, node.name)
            # TODO: self ref edge?
        return g

    def get_all_upstream_dependencies_in_execution_order(self, node: str) -> List[Node]:
        g = self.as_networkx_graph()
        node_names = self._get_all_upstream_dependencies_in_execution_order(g, node)
        return [self.get_node(name) for name in node_names]

    def _get_all_upstream_dependencies_in_execution_order(
        self, g: nx.DiGraph, node: str
    ) -> List[str]:
        nodes = []
        for parent_node in g.predecessors(node):
            parent_deps = self._get_all_upstream_dependencies_in_execution_order(
                g, parent_node
            )
            nodes.extend(parent_deps)
        nodes.append(node)
        return nodes

    def get_all_nodes_in_execution_order(self) -> List[Node]:
        g = self.as_networkx_graph()
        print(g.adj)
        return [self.get_node(name) for name in nx.topological_sort(g)]


# @dataclass(frozen=True)
# class CompiledNodeInput:
#     name: str
#     input: CompiledNode


# CompiledNodeInputs = Dict[str, "CompiledNode"]
#
#
# @dataclass(frozen=True)
# class CompiledNode:
#     env: Environment
#     name: str
#     data_function: DataFunction
#     inputs: Optional[CompiledNodeInputs]
#
#     def __hash__(self):
#         return hash(self.name)


# class CompiledGraph:
#     def __init__(self, declared_graph: DeclaredGraph):
#         self._declared_graph = declared_graph
#         self._nx_graph = nx.DiGraph()
#         self._compile()
#         self._compiled_nodes = {}
#         self._output_dataset_nodes = {}
#         self._processed = set()
#
#     def _compile(self):
#         for dn in self._declared_graph.nodes():
#             self._compile_declared_node(dn)
#
#     def _compile_declared_node(self, dn: Node) -> CompiledNode:
#         if dn in self._processed:
#             return self._compiled_nodes[dn]
#         self._processed.add(dn)
#         if dn.data_function.is_composite:
#             raise
#         dfi = dn.data_function.get_interface()
#         inputs = dfi.assign_inputs(dn.inputs)
#         inputs = {k: dn.env.get_node(v) for k, v in inputs.items()}
#         compiled_inputs: Dict[str, CompiledNode] = {}
#         for annotation in dfi.get_non_recursive_inputs():
#             assert annotation.name in inputs, f"Missing input {annotation}"
#             input_dn = inputs[annotation.name]
#             cn = self._as_compiled_input(annotation, input_dn)
#             compiled_inputs[annotation.name] = cn
#         this = CompiledNode(
#             env=dn.env,
#             name=dn.name,
#             data_function=dn.data_function,
#             inputs=compiled_inputs,
#         )
#         self._add_node(this)
#         for input in dfi.inputs:
#             if input.is_self_ref:
#                 self._add_edge(this, this)
#
#         self._compiled_nodes[dn] = this
#         return this
#
#     def _as_compiled_input(
#         self, annotation: DataFunctionAnnotation, declared_input: Node
#     ) -> CompiledNode:
#         if annotation.data_format_class == "DataBlock":
#             return self._compile_declared_node(declared_input)
#         elif annotation.data_format_class == "DataSet":
#             return self._get_compiled_output_dataset_node(declared_input)
#         else:
#             raise NotImplementedError(annotation.data_format_class)
#
#     def _compile_declared_node_as_dataset(self, dn: Node) -> CompiledNode:
#         cn = self._compile_declared_node(dn)
#
#     def _get_compiled_output_node(self, dn: Node) -> CompiledNode:
#         self._compile_declared_node(dn)
#
#     def _add_edge(self, source: CompiledNode, target: CompiledNode):
#         self._nx_graph.add_edge(source, target)
#
#     def _add_node(self, node: CompiledNode):
#         self._nx_graph.add_node(node)
#         for name, input in node.inputs.items():
#             self._add_edge(input, node)
