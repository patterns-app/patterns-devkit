from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from basis.configuration.base import FrozenPydanticBase, load_yaml, update
from basis.configuration.graph import (
    InterfaceCfg,
    NodeConnection,
    NodeDefinitionCfg,
    GraphNodeCfg,
    GraphDefinitionCfg,
)
from basis.configuration.path import (
    AbsoluteNodeConnection,
    as_absolute_connection,
    join_node_paths,
)
from basis.graph.configured_node import ConfiguredNode, GraphManifest, NodeType


@dataclass
class GraphBuild:
    node: ConfiguredNode
    child_nodes: list[ConfiguredNode]
    # connections: list[AbsoluteNodeConnection]


def configured_nodes_from_yaml(yml_path: str | Path) -> GraphManifest:
    yml_path = Path(yml_path)
    yml_path = yml_path.resolve()
    node_def = NodeDefinitionCfg(**load_yaml(yml_path))
    return graph_as_configured_nodes(node_def, str(Path(yml_path).parent))


def graph_as_configured_nodes(
    root_node: NodeDefinitionCfg, abs_filepath_to_root: str = ""
) -> GraphManifest:
    nodes: list[ConfiguredNode] = []
    # assert root_node.graph is not None, "Graph is empty"
    if root_node.graph is not None:
        for node_cfg in root_node.graph.node_configurations:
            graph_build = build_configured_nodes(
                node_cfg, root_node, abs_filepath_to_root=abs_filepath_to_root
            )
            nodes.append(graph_build.node)
            nodes.extend(graph_build.child_nodes)
    return GraphManifest(graph_name=root_node.name, nodes=nodes)


def build_configured_nodes(
    node_cfg: GraphNodeCfg,
    parent_graph: NodeDefinitionCfg | None = None,
    depth: int = 0,
    absolute_node_path: str = "",
    abs_filepath_to_root: str = "",
) -> GraphBuild:
    configured_nodes = []
    (abs_filepath_to_yaml_config, node_def) = find_node_definition(
        node_cfg.node_definition, abs_filepath_to_root
    )
    node_def_default_name = abs_filepath_to_yaml_config.split("/")[-2]
    node_script_path = None
    if node_def.script is not None:
        node_script_path = str(
            (
                Path(abs_filepath_to_yaml_config).parent
                / node_def.script.script_definition
            ).relative_to(abs_filepath_to_root)
        )
    node_name = node_cfg.name or node_def.name or node_def_default_name
    cfg_node = ConfiguredNode(
        node_name=node_name,
        absolute_node_path=join_node_paths(absolute_node_path, node_name),
        node_depth=depth,
        node_type=NodeType.GRAPH if node_def.graph is not None else NodeType.NODE,
        parent_node=absolute_node_path if absolute_node_path else None,
        node_definition=node_def,
        parameter_values=node_cfg.parameter_values,
        output_aliases=node_cfg.output_aliases,
        file_path_to_yaml_definition_relative_to_root=str(
            Path(abs_filepath_to_yaml_config).relative_to(abs_filepath_to_root)
        ),
        file_path_to_node_script_relative_to_root=node_script_path,
        schedule=node_cfg.schedule,
        labels=node_cfg.labels,
        declared_connections=[],
        flattened_connections=[],
    )
    # child_nodes: list[ConfiguredNode] = []
    connections = []
    if node_def.graph is not None:
        connections.extend(node_def.graph.node_connections)
        for sub_node_cfg in node_def.graph.node_configurations:
            # Recurse
            graph_build = build_configured_nodes(
                sub_node_cfg,
                node_def,
                depth + 1,
                join_node_paths(absolute_node_path, node_name),
                str(Path(abs_filepath_to_yaml_config).parent),
            )
            # child_nodes.append(graph_build.node)
            configured_nodes.append(graph_build.node)
            configured_nodes.extend(graph_build.child_nodes)
    if parent_graph is not None and parent_graph.graph is not None:
        connections.extend(parent_graph.graph.node_connections)
    for connection in connections:
        abs_connection = as_absolute_connection(connection, cfg_node.absolute_node_path)
        # Check for graph port connections
        input_node_path = str(abs_connection.input_path.absolute_node_path)
        output_node_path = str(abs_connection.output_path.absolute_node_path)
        if cfg_node.absolute_node_path in (
            input_node_path,
            output_node_path,
        ):
            cfg_node.declared_connections.append(connection)
            cfg_node.flattened_connections.append(abs_connection)
    return GraphBuild(
        node=cfg_node,
        child_nodes=configured_nodes,
    )


def find_node_definition(
    reference: str, abs_filepath_to_root: str
) -> tuple[str, NodeDefinitionCfg]:
    ref_path = Path(abs_filepath_to_root) / reference
    if reference.endswith(".yaml") or reference.endswith(".yml"):
        yaml_path = ref_path
    else:
        # For now assume it is a dir containing a yaml
        for name in ["node.yml", "node.yaml", "graph.yml", "graph.yaml"]:
            yaml_path = ref_path / name
            if yaml_path.exists():
                break
        else:
            raise NotImplementedError(f"Could not find a yml def in {ref_path}")
    node_def = NodeDefinitionCfg(**load_yaml(yaml_path))
    return str(yaml_path), node_def
