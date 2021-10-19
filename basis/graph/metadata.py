from __future__ import annotations
import sys
from dataclasses import dataclass
from types import ModuleType
import typing
from collections import OrderedDict
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union
from pathlib import Path
import importlib

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.graph import GraphCfg, GraphInterfaceCfg
from basis.configuration.node import NodeCfg
from pydantic.fields import Field

from basis.node.interface import NodeInterface


class GraphMetadata(FrozenPydanticBase):
    name: str
    interface: NodeInterface
    nodes: List[GraphMetadata] = []
    inputs: Dict[str, str] = {}
    parameters: Dict[str, Any] = {}
    original_cfg: Optional[GraphCfg] = None


@dataclass
class GraphMetadataBuilder:
    directory: Path
    cfg: GraphCfg
    nodes: List[Node]

    def get_relpath_as_python_module(self, pth: Union[Path, str]) -> ModuleType:
        sys.path.append(str(self.directory))
        pth = str(self.directory/ pth)
        if pth.endswith(".py"):
            pth = pth[:-3]
        pth.replace("/", ".")
        mod = importlib.import_module(pth)
        sys.path.remove(str(self.directory))
        return mod
    
    def build_graph_metadata_from_config(self) -> GraphMetadata:
        interface = self.cfg.interface
        md = GraphMetadata(name=self.cfg.name, original_cfg=self.cfg,)
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

        


    def find_node(self, pth: str) -> Node:
        for n in self.cfg.nodes:
            if pth.startswith(n.name):