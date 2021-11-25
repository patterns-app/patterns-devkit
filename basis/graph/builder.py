from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Dict

from basis.configuration.base import load_yaml
from basis.configuration.graph import GraphDefinitionCfg, NodeCfg
from basis.configuration.path import DeclaredEdge, AbsoluteEdge, NodeId, PortId
from basis.graph.configured_node import ConfiguredNode, GraphManifest, NodeInterface, OutputDefinition, \
    CURRENT_MANIFEST_SCHEMA_VERSION, NodeType
from basis.node.sql.jinja import parse_interface_from_sql
from basis.utils.ast_parser import read_interface_from_py_node_file


def graph_manifest_from_yaml(yml_path: Path | str) -> GraphManifest:
    return _GraphBuilder(Path(yml_path)).build()


_OutputsByName = Dict[str, Tuple[ConfiguredNode, OutputDefinition]]


class _GraphBuilder:
    def __init__(self, root_graph_location: Path):
        self.root_graph_location = root_graph_location
        self.root_dir = root_graph_location.parent
        self.nodes: List[ConfiguredNode] = []

    def build(self) -> GraphManifest:
        config = self._parse_graph_cfg([], self.root_graph_location)
        return GraphManifest(
            graph_name=config.name or self.root_dir.name,
            manifest_version=CURRENT_MANIFEST_SCHEMA_VERSION,
            nodes=self.nodes
        )

    def _read_graph_yml(self, path: Path) -> GraphDefinitionCfg:
        try:
            text = path.read_text()
        except Exception:
            raise RuntimeError(f'Could not read graph config from {path}')
        return GraphDefinitionCfg(**load_yaml(text))

    def _relative_path_str(self, path: Path) -> str:
        parts = path.relative_to(self.root_dir).parts
        # Always use unix path separators
        return '/'.join(parts)

    def _parse_graph_cfg(self, parents: List[NodeId], node_yaml_path: Path) -> GraphDefinitionCfg:
        config = self._read_graph_yml(node_yaml_path)
        node_dir = node_yaml_path.parent
        outputs_by_name: _OutputsByName = {}

        # step 1: parse all the interfaces
        nodes = [
            self._parse_node_entry(node, node_dir, parents, outputs_by_name) for node in config.nodes
        ]

        # step 2: resolve absolute edges
        for node in nodes:
            self._apply_edges(node, outputs_by_name)

        self.nodes += nodes
        return config

    # set the absolute edges on a node based on its interface and the name mapping in the yaml file
    def _apply_edges(self, node: ConfiguredNode, outputs_by_name: _OutputsByName):
        names = {n.name: n.name for n in node.interface.inputs}
        names.update({e.output_port: e.input_port for e in node.declared_edges if e.output_port in names})
        for i in node.interface.inputs:
            name = names[i.name]
            if name not in outputs_by_name:
                if i.required:
                    raise ValueError(f'Cannot find output named "{name}"')
                continue
            (src, o) = outputs_by_name[name]

            edge = AbsoluteEdge(input_path=PortId(node_id=src.id, port=o.name),
                                output_path=PortId(node_id=node.id, port=i.name))
            node.absolute_edges.append(edge)
            src.absolute_edges.append(edge)

    # record all the output name mappings in this node's config
    def _track_outputs(self, node: ConfiguredNode, outputs: list, mapping: list, outputs_by_name: _OutputsByName):
        names = {n.name: n.name for n in outputs}  # default port names from interface
        names.update({c.src: c.dst for c in (mapping or [])})  # override names from yaml
        for it in names.values():
            if it in outputs_by_name:
                raise ValueError(f'Duplicate output name {it}')
        outputs_by_name.update({names[o.name]: (node, o) for o in outputs})

    def _parse_node_entry(self, node: NodeCfg, node_dir: Path, parents: List[NodeId],
                          outputs_by_name: _OutputsByName) -> ConfiguredNode:
        node_file_path = node_dir / node.node_file
        node_name = node.name or node_file_path.stem
        parent_node_id = parents[-1] if parents else None
        node_id = node.id or NodeId.from_name(node_name, parent_node_id)
        path = parents + [node_id]
        interface, node_type = self._parse_node_interface(node_file_path, path)
        configured_node = ConfiguredNode(
            name=node_name,
            node_type=node_type,
            id=node_id,
            interface=interface,
            node_depth=len(parents),
            description=node.description,
            parent_node_id=parent_node_id,
            file_path_to_node_script_relative_to_root=self._relative_path_str(node_file_path),
            parameter_values=node.parameters or {},
            schedule=node.schedule,
            declared_edges=[
                DeclaredEdge(input_port=m.src, output_port=m.dst)
                for m in ((node.inputs or []) + (node.outputs or []))
            ],
            absolute_edges=[],  # we'll fill this in once we have all the nodes parsed
        )

        self._track_outputs(configured_node, interface.outputs, node.outputs, outputs_by_name)
        return configured_node

    def _parse_node_interface(self, node_file_path: Path, parents: List[str]) -> Tuple[NodeInterface, NodeType]:
        if not node_file_path.exists():
            raise ValueError(f'File {node_file_path} does not exist')
        ext = node_file_path.suffix
        if ext == '.py':
            return self._parse_python_file(node_file_path), NodeType.Node
        elif ext == '.sql':
            return self._parse_sql_file(node_file_path), NodeType.Node
        elif ext in ('.yml', '.yaml'):
            return self._parse_subgraph_yaml(node_file_path, parents), NodeType.Graph
        else:
            raise ValueError(f'Invalid node file type {node_file_path}')

    def _parse_python_file(self, file: Path) -> NodeInterface:
        return read_interface_from_py_node_file(file)

    def _parse_sql_file(self, file: Path) -> NodeInterface:
        return parse_interface_from_sql(file.read_text())

    def _parse_subgraph_yaml(self, file: Path, parents: List[str]) -> NodeInterface:
        raise NotImplementedError('Subgraphs are not currently supported')
