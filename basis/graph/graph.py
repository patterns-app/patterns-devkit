from __future__ import annotations

from basis.configuration.node import NODE_PATH_OUTPUT_SEPARATOR, NodePath


from basis.graph.configured_node import ConfiguredNode
import networkx as nx


def compute_node_digraph(root_node: ConfiguredNode) -> nx.DiGraph:
    g = nx.DiGraph()
    # for n in self.nodes:
    #     g.add_node(n.key)
    #     inputs = n.get_inputs()
    #     for input_node_key in inputs.values():
    #         g.add_node(input_node_key)
    #         g.add_edge(input_node_key, n.key)
    # return g
    node_path_lookup = build_nodepath_lookup(root_node)
    for downstream_node in root_node.nodes:
        for input_name, input_node_path_str in downstream_node.inputs.items():
            input_node = node_path_lookup[input_node_path_str]
            downstream_node_path = NodePath(downstream_node.node_path)
            downstream_node_path.io_name = input_name
            g.add_node( str(downstream_node_path))
            input_node_path = NodePath(input_node.node_path)
            input_node_path.io_name = 
            g.add_node(input_node.node_path)

    return g


def build_nodepath_lookup(root_node: ConfiguredNode) -> dict[str, ConfiguredNode]:
    lookup = {}
    for n in root_node.nodes:
        assert n.node_path not in lookup, "Duplicate node_path detected"
        lookup[n.node_path] = n
        # Recurse:
        lookup.update(build_nodepath_lookup(n))
    return lookup
