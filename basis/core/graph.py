# from typing import Iterable, List, Optional, Set, Tuple, Dict
#
# import networkx as nx
#
# from basis.core.data_function import FunctionNode
# from basis.core.environment import Environment
# from basis.core.typing.object_type import ObjectTypeLike
#
# #
# #
# # class NODEGraph:
# #     nodes: List[ConfiguredDataFunction]
# #     graph: Optional[nx.Graph]
# #
# #     def __init__(self):
# #         self.nodes = []
# #         self.graph = None
# #
# #     def add_nodes(self, nodes: Sequence[ConfiguredDataFunction]):
# #         self.nodes.extend(nodes)
# #         self.build_graph()
# #
# #     def build_graph(self):
# #         self.graph = build_function_graph(self.nodes)
# #
# #     def get_sorted_dependencies(self, node: ConfiguredDataFunction):
# #         sub = nx.subgraph(self.graph, nx.ancestors(self.graph, node.key))
# #         # TODO: cycles?
# #         return nx.topological_sort(sub)
#
#
# def test_function_graph():
#     df = PythonDataFunction(df_t1_source)
#     node = env.add_node("node", df_t1_source)
#     node_chain = env.add_node("node_chain", df_chain, input=node)
#     node2 = env.add_node("node2", df_t1_to_t2, input=DataBlockStream(otype="TestType1"))
#     # nodeg = env.add_node("nodeg", df_generic, input=node2)
#
#     g = FunctionGraph(env, env.all_nodes())
#     assert len(list(g._explicit_graph.adjacency())) == 3
#     print("G", list(g._explicit_graph.adjacency()))
#     assert False
#
#
# class FunctionGraph:
#     _nodes: Dict[str, FunctionNode]
#     graph: Optional[nx.Graph]
#
#     """
#     src1 -> cnf1            -> a1
#                   => DS1
#     src2 -> cnf2            -> a2
#     """
#
#     def __init__(self, env: Environment, nodes: Iterable[FunctionNode]):
#         self.env = env
#         self._nodes = {n.key: n for n in nodes}
#         self._explicit_graph = nx.DiGraph()
#         self.build_explicit_graph()
#         self._resolved_interfaces = {}
#         self._resolved_graph = None
#
#     def resolve_edges(self, node_key: str):
#         node = self.env.get_node(node_key)
#         dfi = node.get_interface()
#         if dfi.output is None:
#             # print(
#             #     f"Root node with no output, if side-effects are a no-no, what does it actually do then?"
#             # )
#             return
#         if dfi.output.is_generic:
#             raise Exception("Generic otype at root")
#         for v in self._explicit_graph.adj[node]:
#             self._explicit_graph
#
#     def resolve_edge_types(self):
#         roots = (v for v, d in self._explicit_graph.in_degree() if d == 0)
#         for root in roots:
#             pass
#
#     # def get_sorted_dependencies(self, node: ConfiguredDataFunction):
#     #     sub = nx.subgraph(self.graph, nx.ancestors(self.graph, node.key))
#     #     # TODO: cycles?
#     #     return nx.topological_sort(sub)
#
#     def add_explicit_edges(
#         self, node: FunctionNode, recursive: bool = True
#     ) -> List[Tuple[str, str]]:
#         edges = []
#         for i in node.get_data_stream_inputs(self.env):
#             print("edge", i)
#             if isinstance(i, FunctionNode):
#                 # edges.append((i.key, node.key))
#                 self._explicit_graph.add_edge(i.key, node.key, otype=None)
#             else:
#                 continue  # TODO: Make sure we are supporting DRStreams properly
#             if recursive:
#                 self.add_explicit_edges(i)
#         return edges
#
#     def build_explicit_graph(self):  # -> nx.Graph:
#         # edges: List[Tuple[str, str]] = []
#         for node in self._nodes:
#             print("visiting", node)
#             if node.is_graph():  # Flatten sub-graphs
#                 sub_nodes = node.get_nodes()
#             else:
#                 sub_nodes = [node]
#             for sub_node in sub_nodes:
#                 # edges.extend(self.add_explicit_edges(sub_node))
#                 self.add_explicit_edges(sub_node)
#             # print(edges)
#         # return nx.from_edgelist(edges, create_using=nx.DiGraph)
#
#
# def get_all_nodes_in_execution_order(
#     env: Environment, as_equivalence_sets: bool = False
# ) -> List[FunctionNode]:
#     if as_equivalence_sets:
#         raise NotImplementedError
#     pass
#
#
# def get_all_upstream_dependencies_in_execution_order(
#     env, node, visited: Set[FunctionNode] = None
# ) -> List[FunctionNode]:
#     if visited is None:
#         visited = set([])
#     if node in visited:
#         return []
#     visited.add(node)
#     dependencies: List[FunctionNode] = []
#     for upstream in node.get_data_stream_inputs(env):
#         upstream_deps = get_all_upstream_dependencies_in_execution_order(env, upstream)
#         dependencies = upstream_deps + dependencies
#     if isinstance(node, FunctionNode):
#         # Ignore DataBlockStream and other non-runnables
#         dependencies.append(node)
#     return dependencies


#
#
# def get_all_nodes_outputting_otype(
#     env: Environment, otype_like: ObjectTypeLike
# ) -> Iterable[FunctionNode]:
#     otype = env.get_otype(otype_like)
#     for node in env.configured_data_function_registry.all():
#         output = node.get_interface().output
#         if not output or output.is_generic:
#             continue
#         output_otype = env.get_otype(output.otype_like)
#         if output and output_otype == otype:
#             yield node
#
#
# def get_all_downstream_dependents_in_execution_order(env, node) -> List[FunctionNode]:
#     raise NotImplementedError
