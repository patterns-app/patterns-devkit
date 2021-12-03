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
    PortType,
    GraphError,
    InputDefinition,
)
from basis.node.sql.jinja import parse_interface_from_sql
from basis.utils.ast_parser import read_interface_from_py_node_file


def graph_manifest_from_yaml(yml_path: Path | str) -> GraphManifest:
    return _GraphBuilder(Path(yml_path)).build()


class NodeParseException(RuntimeError):
    def __init__(self, *args, **kwargs):  # real signature unknown
        super().__init__(*args, **kwargs)


# per-graph node port name mapping and exposed edges
@dataclass
class _Mapping:
    # dict of port name (with mapping applied) -> (node exposing that port, port definition)
    outputs: Dict[str, Tuple[ConfiguredNode, OutputDefinition]]
    # dict of node id -> port name mapping from yaml
    inputs: Dict[NodeId, Dict[str, str]]
    # dicts of exposed port name -> list of edges connecting to that port
    local_edges_by_exposed_input: Dict[str, List[GraphEdge]]
    local_edges_by_exposed_output: Dict[str, List[GraphEdge]]


# node parse result
@dataclass
class _Interface:
    node: NodeInterface
    node_type: NodeType
    # empty for non-graph nodes
    local_edges: List[GraphEdge]


# graph parse result
@dataclass
class _ParsedGraph:
    name: Optional[str]
    interface: NodeInterface
    local_edges: List[GraphEdge]


class _GraphBuilder:
    def __init__(self, root_graph_location: Path):
        self.root_graph_location = root_graph_location
        self.root_dir = root_graph_location.parent
        self.nodes_by_id: Dict[NodeId, ConfiguredNode] = {}
        self.errors: List[GraphError] = []
        self.broken_ports: Set[PortId] = set()

    def build(self) -> GraphManifest:
        parse = self._parse_graph_cfg(self.root_graph_location, None)
        self._resolve_edges()
        return GraphManifest(
            graph_name=parse.name or self.root_dir.name,
            manifest_version=CURRENT_MANIFEST_SCHEMA_VERSION,
            nodes=list(self.nodes_by_id.values()),
            errors=self.errors,
        )

    def _err(self, node_or_id: Union[ConfiguredNode, NodeId], message: str):
        id = node_or_id if isinstance(node_or_id, NodeId) else node_or_id.id
        self.errors.append(GraphError(node_id=id, message=message))

    def _read_graph_yml(self, path: Path) -> GraphDefinitionCfg:
        try:
            text = path.read_text()
        except Exception:
            raise RuntimeError(f"Could not read graph config from {path}")
        return GraphDefinitionCfg(**(load_yaml(text) or {}))

    def _relative_path_str(self, path: Path) -> str:
        parts = path.relative_to(self.root_dir).parts
        # Always use unix path separators
        return "/".join(parts)

    def _parse_graph_cfg(
        self, node_yaml_path: Path, parent: Optional[NodeId]
    ) -> _ParsedGraph:
        config = self._read_graph_yml(node_yaml_path)
        node_dir = node_yaml_path.parent
        exposed_inputs = (
            config.exposes.inputs if config.exposes and config.exposes.inputs else []
        )
        exposed_outputs = (
            config.exposes.outputs if config.exposes and config.exposes.outputs else []
        )
        mapping = _Mapping(
            {}, {}, {i: [] for i in exposed_inputs}, {i: [] for i in exposed_outputs}
        )

        # step 1: parse all the interfaces
        nodes = [
            self._parse_node_entry(node, node_dir, parent, mapping)
            for node in (config.nodes or [])
        ]

        # step 2: set local edges
        self._set_local_input_edges(parent, nodes, mapping)
        self._set_local_exposed_output_edges(parent, mapping)

        self.nodes_by_id.update({node.id: node for node in nodes})

        interface = self._build_graph_interface(
            nodes, mapping, exposed_inputs, exposed_outputs
        )
        local_edges = list(
            chain(
                chain.from_iterable(mapping.local_edges_by_exposed_input.values()),
                chain.from_iterable(mapping.local_edges_by_exposed_output.values()),
            )
        )
        self._check_for_unconnected_exposed_inputs(parent, mapping)

        return _ParsedGraph(config.name, interface, local_edges)

    # create a NodeInterface from a graph node's exposed ports
    def _build_graph_interface(
        self,
        nodes: List[ConfiguredNode],
        mapping: _Mapping,
        exposed_inputs: List[str],
        exposed_outputs: List[str],
    ) -> NodeInterface:
        input_defs_by_name = {
            mapping.inputs[n.id][i.name]: i for n in nodes for i in n.interface.inputs
        }
        # Copy the graph port definitions from the originating node, changing the name to the exposed name
        outputs = [
            mapping.outputs[o][1].copy(update={"name": o})
            for o in exposed_outputs
            if o in mapping.outputs  # may be missing in broken graphs
        ]
        inputs = [
            input_defs_by_name[i].copy(update={"name": i})
            for i in exposed_inputs
            if i in input_defs_by_name  # may be missing in broken graphs
        ]
        return NodeInterface(inputs=inputs, outputs=outputs, parameters=[])

    # set local edges connected to nodes' inputs
    def _set_local_input_edges(
        self, graph_id: Optional[NodeId], nodes: List[ConfiguredNode], mapping: _Mapping
    ):
        for node in nodes:
            for i in node.interface.inputs:
                name = mapping.inputs[node.id][i.name]
                if name in mapping.outputs:
                    (src, o) = mapping.outputs[name]
                    self._set_edge(
                        src.id,
                        o.name,
                        node.id,
                        i.name,
                        node.local_edges,
                        src.local_edges,
                    )
                elif name in mapping.local_edges_by_exposed_input:
                    self._set_edge(
                        graph_id,
                        name,
                        node.id,
                        i.name,
                        node.local_edges,
                        mapping.local_edges_by_exposed_input[name],
                    )
                elif i.required:
                    self._err(node, f'Cannot find output named "{name}"')
                    self.broken_ports.add(PortId(node_id=node.id, port=i.name))

    # set local edges on any nodes connected to exposed outputs
    def _set_local_exposed_output_edges(
        self, graph_id: Optional[NodeId], mapping: _Mapping
    ):
        for (name, edges) in mapping.local_edges_by_exposed_output.items():
            if name in mapping.outputs:
                assert graph_id is not None
                (src, o) = mapping.outputs[name]
                self._set_edge(src.id, o.name, graph_id, name, edges, src.local_edges)
            elif graph_id is not None:
                self._err(graph_id, f'Exposed output does not exist: "{name}"')
            else:
                # we'll have to revisit this if we want to support manifests for components
                raise ValueError("Cannot expose outputs on root graph")

    def _set_edge(
        self,
        src_id: NodeId,
        src_name: str,
        dst_id: NodeId,
        dst_name: str,
        *all_edges: List[GraphEdge],
    ):
        assert src_id is not None
        assert dst_id is not None
        edge = GraphEdge(
            input=PortId(node_id=src_id, port=src_name),
            output=PortId(node_id=dst_id, port=dst_name),
        )
        for edges in all_edges:
            edges.append(edge)

    def _check_edge(
        self, dst_id: NodeId, name: str, src_t: PortType, dst_t: PortType,
    ):
        if src_t != dst_t:
            self._err(
                dst_id,
                f"Cannot connect {name}: input is a {src_t}, but output is a {dst_t}",
            )

    def _check_for_unconnected_exposed_inputs(
        self, graph_id: Optional[NodeId], mapping: _Mapping
    ):
        for name, edges in mapping.local_edges_by_exposed_input.items():
            if not edges:
                self._err(graph_id, f'Exposed input does not exist: "{name}"')

    # record all the output name mappings in this node's config
    def _track_outputs(
        self,
        node: ConfiguredNode,
        outputs: List[OutputDefinition],
        mappings: List[PortMappingCfg],
        mapping: _Mapping,
    ):
        name_map = {c.src: c.dst for c in mappings}  # overrides from yaml
        for o in outputs:
            name = name_map.get(o.name, o.name)
            if name in mapping.outputs:
                (other_node, other_def) = mapping.outputs[name]
                self._err(node, f"Duplicate output '{name}'")
                self._err(other_node, f"Duplicate output '{name}'")
                self.broken_ports.add(PortId(node_id=node.id, port=o.name))
                self.broken_ports.add(
                    PortId(node_id=other_node.id, port=other_def.name)
                )
            else:
                mapping.outputs[name] = (node, o)

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
        for node in self.nodes_by_id.values():
            if node.node_type == NodeType.Graph:
                continue
            for local_edge in node.local_input_edges():
                self._resolve_edge(local_edge, node)

    def _resolve_edge(self, local_edge: GraphEdge, node: ConfiguredNode):
        vertex_port = local_edge.input
        vertex = self.nodes_by_id[vertex_port.node_id]

        if vertex_port in self.broken_ports:
            return  # already reported an error for this edge

        while vertex.node_type == NodeType.Graph:
            vertex_port = next(
                (e.input for e in vertex.local_edges if e.output == vertex_port), None,
            )
            if not vertex_port:
                self._err(
                    node, f"Could not resolve edge for port {local_edge.input.port}"
                )
                return
            vertex = self.nodes_by_id[vertex_port.node_id]

        self._set_edge(
            vertex_port.node_id,
            vertex_port.port,
            local_edge.output.node_id,
            local_edge.output.port,
            node.resolved_edges,
            vertex.resolved_edges,
        )
        src_t = next(
            i.port_type
            for i in node.interface.inputs
            if i.name == local_edge.output.port
        )
        dst_t = next(
            i.port_type for i in vertex.interface.outputs if i.name == vertex_port.port
        )
        self._check_edge(
            local_edge.output.node_id, local_edge.output.port, src_t, dst_t
        )

    def _default_node_name(self, path: Path) -> str:
        # For 'graph.yml' files, use the directory name
        if path.suffix in (".yml", ".yaml"):
            return path.parent.name
        # For py and sql files, use the file name
        return path.stem

    def _parse_node_entry(
        self,
        node: NodeCfg,
        node_dir: Path,
        parent: Optional[NodeId],
        mapping: _Mapping,
    ) -> ConfiguredNode:
        node_file_path = node_dir / node.node_file
        node_name = node.name or self._default_node_name(node_file_path)
        node_id = node.id or NodeId.from_name(node_name, parent)
        relative_node_path = self._relative_path_str(node_file_path)
        if node_id in self.nodes_by_id:
            raise ValueError(f"Duplicate node id: {node_id}")
        try:
            interface = self._parse_node_interface(node_file_path, node_id)
            parse_err = False
        except NodeParseException as e:
            interface = _Interface(
                node=NodeInterface(inputs=[], outputs=[], parameters=[]),
                node_type=NodeType.Node,
                local_edges=[]
            )
            parse_err = True
            self._err(node_id, f'Error parsing file {relative_node_path}')

        configured_node = ConfiguredNode(
            name=node_name,
            node_type=interface.node_type,
            id=node_id,
            interface=interface.node,
            description=node.description,
            parent_node_id=parent,
            file_path_to_node_script_relative_to_root=relative_node_path,
            parameter_values=node.parameters or {},
            schedule=node.schedule,
            local_edges=interface.local_edges,
            resolved_edges=[],  # we'll fill these in once we have all the nodes parsed
        )

        self._track_outputs(
            configured_node, interface.node.outputs, node.outputs or [], mapping
        )
        self._track_inputs(node_id, interface.node.inputs, node.inputs, mapping)
        return configured_node

    def _parse_node_interface(
        self, node_file_path: Path, parent: Optional[NodeId]
    ) -> _Interface:
        if not node_file_path.exists():
            raise ValueError(f"File {node_file_path} does not exist")

        ext = node_file_path.suffix
        if ext == ".py":
            interface = self._parse_python_file(node_file_path)
        elif ext == ".sql":
            interface = self._parse_sql_file(node_file_path)
        elif ext in (".yml", ".yaml"):
            interface = self._parse_subgraph_yaml(node_file_path, parent)
        else:
            raise ValueError(f"Invalid node file type {node_file_path}")
        return interface

    def _parse_python_file(self, file: Path) -> _Interface:
        try:
            node_file = read_interface_from_py_node_file(file)
        except Exception as e:
            raise NodeParseException from e

        return _Interface(node_file, NodeType.Node, [])

    def _parse_sql_file(self, file: Path) -> _Interface:
        try:
            sql = parse_interface_from_sql(file.read_text())
        except Exception as e:
            raise NodeParseException from e
        return _Interface(sql, NodeType.Node, [])

    def _parse_subgraph_yaml(self, file: Path, parent: Optional[NodeId]) -> _Interface:
        parse = self._parse_graph_cfg(file, parent)
        return _Interface(parse.interface, NodeType.Graph, parse.local_edges)
