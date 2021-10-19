from __future__ import annotations
import sys
from dataclasses import dataclass
from types import ModuleType
import typing
from collections import OrderedDict
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union
from pathlib import Path
import importlib
from contextlib import contextmanager

from basis.configuration.base import FrozenPydanticBase
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
    nodes: List[ConfiguredNode]

    def build_graph_metadata_from_config(self) -> ConfiguredNode:
        interface = self.cfg.interface
        md = ConfiguredNode(name=self.cfg.name, original_cfg=self.cfg,)
        return md

    def node_interface_from_cfg(self, cfg: GraphInterfaceCfg) -> NodeInterface:
        inputs = OrderedDict()
        for i in cfg.inputs:
            assert i.like is not None, "Must specify `like` for input"
            self.find_node(i.like)

        outputs = OrderedDict()
        parameters = OrderedDict()
        return NodeInterface(inputs=inputs, outputs=outputs, parameters=parameters,)

    def load_nodes(self):
        nodes = []
        for n in self.cfg.nodes:
            if n.python:
                node = self.load_python_node(n.python)
            elif n.sql:
                node = self.load_sql_node(n.sql)
            elif n.subgraph:
                node = self.load_subgraph_node(n.subgraph)
            else:
                raise ValueError(n)
        nodes.append(node)
        return nodes

    @contextmanager
    def local_python_path(self):
        sys.path.append(str(self.directory))
        yield
        sys.path.remove(str(self.directory))

    def load_python_node(self, pth: str) -> Node:
        with self.local_python_path():
            node = single_of_type_in_path(pth, Node)
            return node

    def find_node_reference(self, ref: str) -> ConfiguredNode:
        pass
