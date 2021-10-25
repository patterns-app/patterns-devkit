from __future__ import annotations


from basis.graph.configured_node import ConfiguredNode


def compute_node_adjacency_list(root_node: ConfiguredNode) -> dict[str, list[str]]:
    node_path_lookup = build_nodepath_lookup(root_node)
    adj_list: dict[str, list[str]] = {}
    for n in root_node.nodes:
        for input_name, input_node_path in n.inputs.items():
            adj_list[input_node_path] = n.node_path

    return adj_list


def build_nodepath_lookup(root_node: ConfiguredNode) -> dict[str, ConfiguredNode]:
    lookup = {}
    for n in root_node.nodes:
        assert n.node_path not in lookup, "Duplicate node_path detected"
        lookup[n.node_path] = n
        # Recurse:
        lookup.update(build_nodepath_lookup(n))
    return lookup
