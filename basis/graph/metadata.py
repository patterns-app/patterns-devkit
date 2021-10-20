from __future__ import annotations
import sys
import os
from dataclasses import dataclass
from types import ModuleType
import typing
from collections import OrderedDict
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union
from pathlib import Path
import importlib
from contextlib import contextmanager
from basis.configuration import graph

from basis.configuration.base import FrozenPydanticBase, load_yaml
from basis.configuration.graph import GraphCfg, GraphInterfaceCfg
from basis.configuration.node import GraphNodeCfg
from pydantic.fields import Field

from basis.node.interface import NodeInterface
from basis.node.node import Node
from basis.utils.modules import single_of_type_in_path


class ConfiguredNode(FrozenPydanticBase):
    name: str
    interface: NodeInterface
    nodes: List[ConfiguredNode] = []
    inputs: Dict[str, str] = {}
    parameters: Dict[str, Any] = {}
    original_cfg: Optional[Union[GraphCfg, GraphNodeCfg]] = None


@dataclass
class ConfiguredGraphBuilder:
    directory: Path
    cfg: GraphCfg
    configured_nodes: Optional[List[ConfiguredNode]] = None
    parent: Optional[ConfiguredGraphBuilder] = None

    def build_metadata_from_config(self) -> ConfiguredNode:
        configured_nodes = self.build_nodes()
        interface = self.build_node_interface()
        md = ConfiguredNode(
            name=self.cfg.name,
            interface=interface,
            nodes=configured_nodes,
            original_cfg=self.cfg,
            # TODO: inputs and parameters at top-level? What about nested?
        )
        return md

    def build_node_interface(self) -> NodeInterface:
        # TODO: what if no interface declared? then we must infer from children
        # TODO: need interface.merge operation or similar
        inputs = OrderedDict()
        for i in self.cfg.interface.inputs:
            assert i.like is not None, "Must specify `like` for input"
            self.find_node(i.like)

        outputs = OrderedDict()
        parameters = OrderedDict()
        return NodeInterface(inputs=inputs, outputs=outputs, parameters=parameters,)

    def build_nodes(self) -> List[ConfiguredNode]:
        configured_nodes = []
        for graph_node_cfg in self.cfg.nodes:
            if graph_node_cfg.python:
                cfg_node = self.build_python_configured_node(graph_node_cfg)
            elif graph_node_cfg.sql:
                cfg_node = self.build_sql_configured_node(graph_node_cfg)
            elif graph_node_cfg.subgraph:
                cfg_node = self.build_subgraph_configured_node(graph_node_cfg)
            else:
                raise ValueError(graph_node_cfg)
            configured_nodes.append(cfg_node)
        return configured_nodes

    def build_python_configured_node(
        self, graph_node_cfg: GraphNodeCfg
    ) -> ConfiguredNode:
        assert graph_node_cfg.python is not None
        node = self.load_python_node(graph_node_cfg.python)
        return self.build_configured_node_from_node(graph_node_cfg, node)

    def build_sql_configured_node(self, graph_node_cfg: GraphNodeCfg) -> ConfiguredNode:
        assert graph_node_cfg.sql is not None
        node = self.load_sql_node(graph_node_cfg.sql)
        return self.build_configured_node_from_node(graph_node_cfg, node)

    def build_configured_node_from_node(
        self, graph_node_cfg: GraphNodeCfg, node: Node
    ) -> ConfiguredNode:
        cfg_node = ConfiguredNode(
            name=node.name,
            interface=node.interface,
            inputs=graph_node_cfg.inputs,
            parameters=graph_node_cfg.parameters,
            original_cfg=graph_node_cfg,
        )
        return cfg_node

    def build_subgraph_configured_node(
        self, graph_node_cfg: GraphNodeCfg
    ) -> ConfiguredNode:
        assert graph_node_cfg.subgraph is not None
        pth = self.directory / graph_node_cfg.subgraph
        cfg = self.load_graph_cfg(str(pth))
        builder = ConfiguredGraphBuilder(directory=pth, cfg=cfg, parent=self,)
        return builder.build_metadata_from_config()

    @contextmanager
    def set_current_path(self):
        old_dr = os.curdir
        os.chdir(self.directory)
        sys.path.append(str(self.directory))
        yield
        sys.path.remove(str(self.directory))
        os.chdir(old_dr)

    def load_python_node(self, relpath: str) -> Node:
        with self.set_current_path():
            node = single_of_type_in_path(relpath, Node)
            return node

    def load_graph_cfg(self, relpath: str) -> GraphCfg:
        with self.set_current_path():
            data = load_yaml(relpath)
        return GraphCfg(**data)

    def load_sql_node(self, relpth: str) -> Node:
        # TODO:
        return build_node_from_sql_file(relpath)

    def find_node_reference(self, ref: str) -> ConfiguredNode:
        pass
