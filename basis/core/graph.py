from typing import Iterable, List, Optional, Sequence, Set, Tuple

import networkx as nx

from basis.core.data_function import ConfiguredDataFunction
from basis.core.environment import Environment
from basis.core.object_type import ObjectType, ObjectTypeLike

# Unused atm, will eventually want for compiled function graphs
# def get_edges(
#     env: Environment, cdf: ConfiguredDataFunction, recursive: bool = True
# ) -> List[Tuple[str, str]]:
#     edges = []
#     for i in cdf.get_data_stream_inputs(env):
#         if isinstance(i, ConfiguredDataFunction):
#             edges.append((i.key, cdf.key))
#         else:
#             raise NotImplementedError  # TODO: support DRStreams etc
#         if recursive:
#             edges.extend(get_edges(i))
#     return edges
#
#
# def build_function_graph(
#     nodes: Sequence[ConfiguredDataFunction], recursive: bool = True
# ) -> nx.Graph:
#     edges: List[Tuple[str, str]] = []
#     for cdf in nodes:
#         edges.extend(get_edges(cdf, recursive=recursive))
#     return nx.from_edgelist(edges, create_using=nx.DiGraph)
#
#
# class CDFGraph:
#     nodes: List[ConfiguredDataFunction]
#     graph: Optional[nx.Graph]
#
#     def __init__(self):
#         self.nodes = []
#         self.graph = None
#
#     def add_nodes(self, nodes: Sequence[ConfiguredDataFunction]):
#         self.nodes.extend(nodes)
#         self.build_graph()
#
#     def build_graph(self):
#         self.graph = build_function_graph(self.nodes)
#
#     def get_sorted_dependencies(self, cdf: ConfiguredDataFunction):
#         sub = nx.subgraph(self.graph, nx.ancestors(self.graph, cdf.key))
#         # TODO: cycles?
#         return nx.topological_sort(sub)


def get_all_upstream_dependencies_in_execution_order(
    env, node, visited: Set[ConfiguredDataFunction] = None
) -> List[ConfiguredDataFunction]:
    if visited is None:
        visited = set([])
    if node in visited:
        return []
    visited.add(node)
    dependencies: List[ConfiguredDataFunction] = []
    for upstream in node.get_data_stream_inputs(env):
        upstream_deps = get_all_upstream_dependencies_in_execution_order(env, upstream)
        dependencies = upstream_deps + dependencies
    if isinstance(node, ConfiguredDataFunction):
        # Ignore DataResourceStream and other non-runnables
        dependencies.append(node)
    return dependencies


def get_all_nodes_outputting_otype(
    env: Environment, otype_like: ObjectTypeLike
) -> Iterable[ConfiguredDataFunction]:
    otype = env.get_otype(otype_like)
    for node in env.configured_data_function_registry.all():
        output = node.get_interface().output
        if not output or output.is_generic:
            continue
        output_otype = env.get_otype(output.otype_like)
        if output and output_otype == otype:
            yield node


def get_all_downstream_dependents_in_execution_order(
    env, node
) -> List[ConfiguredDataFunction]:
    raise NotImplementedError
