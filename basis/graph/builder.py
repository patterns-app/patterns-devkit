from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict, Set, Optional
from itertools import chain
from basis.configuration.base import load_yaml
from basis.configuration.graph import GraphDefinitionCfg, NodeCfg, PortMappingCfg
from basis.configuration.path import GraphEdge, NodeId, PortId
from basis.graph.configured_node import (
    ConfiguredNode,
    GraphManifest,
    NodeInterface,
    OutputDefinition,
    CURRENT_MANIFEST_SCHEMA_VERSION,
    NodeType,
    InputDefinition,
)
from basis.node.sql.jinja import parse_interface_from_sql
from basis.utils.ast_parser import read_interface_from_py_node_file


def graph_manifest_from_yaml(yml_path: Path | str) -> GraphManifest:
    return _GraphBuilder(Path(yml_path)).build()


_OutputsByName = Dict[str, Tuple[ConfiguredNode, OutputDefinition]]
_InputMappingByNodeId = Dict[NodeId, Dict[str, str]]


@dataclass
class _Mapping:
    outputs: _OutputsByName
    inputs: _InputMappingByNodeId
    local_edges_by_exposed_input: Dict[str, List[GraphEdge]]
    local_edges_by_exposed_output: Dict[str, List[GraphEdge]]


@dataclass
class _Interface:
    node: NodeInterface
    node_type: NodeType
    # empty for non-graph nodes
    local_edges: List[GraphEdge]


@dataclass
class _ParsedGraph:
    name: Optional[str]
    interface: NodeInterface
    local_edges: List[GraphEdge]


class _GraphBuilder:
    def __init__(self, root_graph_location: Path):
        self.root_graph_location = root_graph_location
        self.root_dir = root_graph_location.parent
        self.nodes: List[ConfiguredNode] = []
        self._interfaces_by_file: Dict[Path, _Interface] = {}

    def build(self) -> GraphManifest:
        parse = self._parse_graph_cfg(self.root_graph_location, None)
        self._resolve_edges()
        return GraphManifest(
            graph_name=parse.name or self.root_dir.name,
            manifest_version=CURRENT_MANIFEST_SCHEMA_VERSION,
            nodes=self.nodes,
        )

    def _read_graph_yml(self, path: Path) -> GraphDefinitionCfg:
        try:
            text = path.read_text()
        except Exception:
            raise RuntimeError(f"Could not read graph config from {path}")
        return GraphDefinitionCfg(**load_yaml(text))

    def _relative_path_str(self, path: Path) -> str:
        parts = path.relative_to(self.root_dir).parts
        # Always use unix path separators
        return "/".join(parts)

    def _parse_graph_cfg(self, node_yaml_path: Path, parent: Optional[NodeId]) -> _ParsedGraph:
        config = self._read_graph_yml(node_yaml_path)
        node_dir = node_yaml_path.parent
        exposed_inputs = config.exposes.inputs if config.exposes and config.exposes.inputs else []
        exposed_outputs = config.exposes.outputs if config.exposes and config.exposes.outputs else []
        mapping = _Mapping({}, {}, {i: [] for i in exposed_inputs}, {i: [] for i in exposed_outputs})

        # step 1: parse all the interfaces
        nodes = [
            self._parse_node_entry(node, node_dir, parent, mapping)
            for node in config.nodes
        ]

        # step 2: set local edges
        self._set_local_input_edges(parent, nodes, mapping)
        self._set_local_exposed_output_edges(parent, mapping)

        self.nodes += nodes

        interface = self._build_graph_interface(nodes, mapping, exposed_inputs, exposed_outputs)
        local_edges = list(chain(
            chain.from_iterable(mapping.local_edges_by_exposed_input.values()),
            chain.from_iterable(mapping.local_edges_by_exposed_output.values()),
        ))

        return _ParsedGraph(config.name, interface, local_edges)

    # create a NodeInterface from a graph node's exposed ports
    def _build_graph_interface(self, nodes: List[ConfiguredNode], mapping: _Mapping, exposed_inputs: List[str],
                               exposed_outputs: List[str]) -> NodeInterface:
        input_defs_by_name = {mapping.inputs[n.id][i.name]: i for n in nodes for i in n.interface.inputs}
        # Copy the graph port definitions from the originating node, changing the name to the exposed name
        outputs = [mapping.outputs[o][1].copy(update={'name': o}) for o in exposed_outputs]
        inputs = [input_defs_by_name[i].copy(update={'name': i}) for i in exposed_inputs]
        return NodeInterface(inputs=inputs, outputs=outputs, parameters=[])

    # set local edges connected to nodes' inputs
    def _set_local_input_edges(self, graph_id: Optional[NodeId], nodes: List[ConfiguredNode], mapping: _Mapping):
        for node in nodes:
            for i in node.interface.inputs:
                name = mapping.inputs[node.id][i.name]
                if name in mapping.outputs:
                    (src, o) = mapping.outputs[name]
                    self._set_edge(src.id, o.name, node.id, i.name, node.local_edges, src.local_edges)
                elif name in mapping.local_edges_by_exposed_input:
                    self._set_edge(graph_id, name, node.id, i.name, node.local_edges,
                                   mapping.local_edges_by_exposed_input[name])
                elif i.required:
                    raise ValueError(f'Cannot find output named "{name}"')

    # set local edges on any nodes connected to exposed outputs
    def _set_local_exposed_output_edges(self, graph_id: Optional[NodeId], mapping: _Mapping):
        for (name, edges) in mapping.local_edges_by_exposed_output.items():
            if name in mapping.outputs:
                (src, o) = mapping.outputs[name]
                self._set_edge(src.id, o.name, graph_id, name, edges, src.local_edges)
            else:
                raise ValueError(f'Cannot find output named "{name}"')

    def _set_edge(self, src_id: NodeId, src_name: str, dst_id: NodeId, dst_name: str,
                  *all_edges: List[GraphEdge]):
        assert src_id is not None
        assert dst_id is not None
        edge = GraphEdge(
            input=PortId(node_id=src_id, port=src_name),
            output=PortId(node_id=dst_id, port=dst_name),
        )
        for edges in all_edges:
            edges.append(edge)

    # record all the output name mappings in this node's config
    def _track_outputs(
        self,
        node: ConfiguredNode,
        outputs: List[OutputDefinition],
        mappings: List[PortMappingCfg],
        mapping: _Mapping,
    ):
        names = {n.name: n.name for n in outputs}  # defaults from interface
        names.update({c.src: c.dst for c in (mappings or [])})  # override from yaml
        for it in names.values():
            if it in mapping.outputs:
                raise ValueError(f"Duplicate output name {it}")
        mapping.outputs.update({names[o.name]: (node, o) for o in outputs})

    def _track_inputs(
        self,
        node_id: NodeId,
        inputs: List[InputDefinition],
        mappings: List[PortMappingCfg],
        mapping: _Mapping,
    ):
        names = {n.name: n.name for n in inputs}
        names.update({c.dst: c.src for c in (mappings or [])})
        mapping.inputs[node_id] = names

    def _resolve_edges(self):
        nodes_by_id: Dict[NodeId, ConfiguredNode] = {node.id: node for node in self.nodes}
        for node in self.nodes:
            if node.node_type == NodeType.Graph:
                continue
            for local_edge in node.local_input_edges():
                vertex_port = local_edge.input
                vertex = nodes_by_id[vertex_port.node_id]
                while vertex.node_type == NodeType.Graph:
                    vertex_port = next(
                        e.input
                        for e in vertex.local_edges
                        if e.output == vertex_port
                    )
                    vertex = nodes_by_id[vertex_port.node_id]
                self._set_edge(vertex_port.node_id, vertex_port.port, local_edge.output.node_id, local_edge.output.port,
                               node.resolved_edges, vertex.resolved_edges)

    def _default_node_name(self, path: Path) -> str:
        # For 'graph.yml' files, use the directory name
        if path.suffix in ('.yml', '.yaml'):
            return path.parent.name
        # For py and sql files, use the file name
        return path.stem

    def _parse_node_entry(
        self, node: NodeCfg, node_dir: Path, parent: Optional[NodeId], mapping: _Mapping,
    ) -> ConfiguredNode:
        node_file_path = node_dir / node.node_file
        node_name = node.name or self._default_node_name(node_file_path)
        node_id = node.id or NodeId.from_name(node_name, parent)
        interface = self._get_node_interface(node_file_path, node_id)
        configured_node = ConfiguredNode(
            name=node_name,
            node_type=interface.node_type,
            id=node_id,
            interface=interface.node,
            description=node.description,
            parent_node_id=parent,
            file_path_to_node_script_relative_to_root=self._relative_path_str(
                node_file_path
            ),
            parameter_values=node.parameters or {},
            schedule=node.schedule,
            local_edges=interface.local_edges,
            resolved_edges=[],  # we'll fill these in once we have all the nodes parsed
        )

        self._track_outputs(configured_node, interface.node.outputs, node.outputs, mapping)
        self._track_inputs(node_id, interface.node.inputs, node.inputs, mapping)
        return configured_node

    def _get_node_interface(self, node_file_path: Path, parent: Optional[NodeId]) -> _Interface:
        if not node_file_path.exists():
            raise ValueError(f"File {node_file_path} does not exist")
        if node_file_path in self._interfaces_by_file:
            return self._interfaces_by_file[node_file_path]

        ext = node_file_path.suffix
        if ext == ".py":
            interface = self._parse_python_file(node_file_path)
        elif ext == ".sql":
            interface = self._parse_sql_file(node_file_path)
        elif ext in (".yml", ".yaml"):
            interface = self._parse_subgraph_yaml(node_file_path, parent)
        else:
            raise ValueError(f"Invalid node file type {node_file_path}")
        self._interfaces_by_file[node_file_path] = interface
        return interface

    def _parse_python_file(self, file: Path) -> _Interface:
        return _Interface(read_interface_from_py_node_file(file), NodeType.Node, [])

    def _parse_sql_file(self, file: Path) -> _Interface:
        return _Interface(parse_interface_from_sql(file.read_text()), NodeType.Node, [])

    def _parse_subgraph_yaml(self, file: Path, parent: Optional[NodeId]) -> _Interface:
        parse = self._parse_graph_cfg(file, parent)
        return _Interface(parse.interface, NodeType.Graph, parse.local_edges)
