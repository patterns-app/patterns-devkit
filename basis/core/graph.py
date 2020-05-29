from typing import Iterable, List, Optional, Set, Tuple

import networkx as nx

from basis.core.data_function import ConfiguredDataFunction
from basis.core.environment import Environment
from basis.core.typing.object_type import ObjectTypeLike

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


class FunctionGraph:
    nodes: List[ConfiguredDataFunction]
    graph: Optional[nx.Graph]

    """
    src1 -> cnf1            -> a1
                  => DS1
    src2 -> cnf2            -> a2
    """

    def __init__(self, env: Environment, nodes: Iterable[ConfiguredDataFunction]):
        self.env = env
        self._nodes = nodes
        self._explicit_graph = self.build_explicit_graph()
        self._resolved_interfaces = {}
        self._resolved_graph = None

    def resolve_type_interfaces(self):
        pass

    # def get_sorted_dependencies(self, cdf: ConfiguredDataFunction):
    #     sub = nx.subgraph(self.graph, nx.ancestors(self.graph, cdf.key))
    #     # TODO: cycles?
    #     return nx.topological_sort(sub)

    def get_explicit_edges(
        self, cdf: ConfiguredDataFunction, recursive: bool = True
    ) -> List[Tuple[str, str]]:
        edges = []
        for i in cdf.get_data_stream_inputs(self.env):
            print("edge", i)
            if isinstance(i, ConfiguredDataFunction):
                edges.append((i.key, cdf.key))
            else:
                continue  # TODO: Make sure we are supporting DRStreams properly
            if recursive:
                edges.extend(self.get_explicit_edges(i))
        return edges

    def build_explicit_graph(self) -> nx.Graph:
        edges: List[Tuple[str, str]] = []
        for cdf in self._nodes:
            print("visiting", cdf)
            if cdf.is_graph():  # Flatten sub-graphs
                sub_cdfs = cdf.get_cdfs()
            else:
                sub_cdfs = [cdf]
            for sub_cdf in sub_cdfs:
                edges.extend(self.get_explicit_edges(sub_cdf))
            print(edges)
        return nx.from_edgelist(edges, create_using=nx.DiGraph)


def get_all_nodes_in_execution_order(
    env: Environment, as_equivalence_sets: bool = False
) -> List[ConfiguredDataFunction]:
    if as_equivalence_sets:
        raise NotImplementedError
    pass


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
