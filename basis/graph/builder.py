from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict

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


@dataclass
class _Interface:
    node: NodeInterface
    node_type: NodeType


class _GraphBuilder:
    def __init__(self, root_graph_location: Path):
        self.root_graph_location = root_graph_location
        self.root_dir = root_graph_location.parent
        self.nodes: List[ConfiguredNode] = []
        self._interfaces_by_file: Dict[Path, _Interface] = {}

    def build(self) -> GraphManifest:
        config = self._parse_graph_cfg([], self.root_graph_location)
        return GraphManifest(
            graph_name=config.name or self.root_dir.name,  # todo:need name
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

    def _parse_graph_cfg(
        self, parents: List[NodeId], node_yaml_path: Path
    ) -> GraphDefinitionCfg:
        config = self._read_graph_yml(node_yaml_path)
        node_dir = node_yaml_path.parent
        mapping = _Mapping({}, {})

        # step 1: parse all the interfaces
        nodes = [
            self._parse_node_entry(node, node_dir, parents, mapping)
            for node in config.nodes
        ]

        # step 2: resolve absolute edges
        for node in nodes:
            self._apply_edges(node, mapping)

        self.nodes += nodes
        return config

    # set the absolute edges on a node based on its interface and the name mapping in the yaml file
    def _apply_edges(self, node: ConfiguredNode, mapping: _Mapping):
        for i in node.interface.inputs:
            name = mapping.inputs[node.id][i.name]
            if name not in mapping.outputs:
                if i.required:
                    raise ValueError(f'Cannot find output named "{name}"')
                continue
            (src, o) = mapping.outputs[name]

            edge = GraphEdge(
                input_path=PortId(node_id=src.id, port=o.name),
                output_path=PortId(node_id=node.id, port=i.name),
            )
            node.local_edges.append(edge)
            src.local_edges.append(edge)

            # TODO: resolve
            node.resolved_edges.append(edge)
            src.resolved_edges.append(edge)

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

    def _parse_node_entry(
        self, node: NodeCfg, node_dir: Path, parents: List[NodeId], mapping: _Mapping,
    ) -> ConfiguredNode:
        node_file_path = node_dir / node.node_file
        node_name = node.name or node_file_path.stem
        parent_node_id = parents[-1] if parents else None
        node_id = node.id or NodeId.from_name(node_name, parent_node_id)
        interface = self._get_node_interface(node_file_path, parents + [node_id])
        configured_node = ConfiguredNode(
            name=node_name,
            node_type=interface.node_type,
            id=node_id,
            interface=interface.node,
            node_depth=len(parents),
            description=node.description,
            parent_node_id=parent_node_id,
            file_path_to_node_script_relative_to_root=self._relative_path_str(
                node_file_path
            ),
            parameter_values=node.parameters or {},
            schedule=node.schedule,
            local_edges=[],  # we'll fill these in once we have all the nodes parsed
            resolved_edges=[],
        )

        self._track_outputs(
            configured_node, interface.node.outputs, node.outputs, mapping
        )
        self._track_inputs(node_id, interface.node.inputs, node.inputs, mapping)
        return configured_node

    def _get_node_interface(
        self, node_file_path: Path, parents: List[str]
    ) -> _Interface:
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
            interface = self._parse_subgraph_yaml(node_file_path, parents)
        else:
            raise ValueError(f"Invalid node file type {node_file_path}")
        self._interfaces_by_file[node_file_path] = interface
        return interface

    def _parse_python_file(self, file: Path) -> _Interface:
        return _Interface(read_interface_from_py_node_file(file), NodeType.Node)

    def _parse_sql_file(self, file: Path) -> _Interface:
        return _Interface(parse_interface_from_sql(file.read_text()), NodeType.Node)

    def _parse_subgraph_yaml(self, file: Path, parents: List[str]) -> _Interface:
        node_id = parents[-1]
        raise NotImplementedError("Subgraphs are not currently supported")
