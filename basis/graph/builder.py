from __future__ import annotations

import importlib
import os
import sys
import typing
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration import graph
from basis.configuration.base import FrozenPydanticBase, load_yaml
from basis.configuration.graph import GraphCfg, GraphInterfaceCfg
from basis.configuration.node import GraphNodeCfg, NodeType
from basis.graph.configured_node import ConfiguredNode
from basis.node.interface import (
    IoBase,
    NodeInterface,
    Parameter,
    ParameterType,
    merge_interfaces,
)
from basis.node.node import Node, parse_node_output_path
from basis.node.sql.jinja import parse_interface_from_sql
from basis.utils.modules import single_of_type_in_path
from pydantic.fields import Field


@dataclass
class ConfiguredGraphBuilder:
    directory: Path
    cfg: GraphCfg
    configured_nodes: Optional[List[ConfiguredNode]] = None
    parent: Optional[ConfiguredGraphBuilder] = None

    def build_manifest_from_config(self) -> ConfiguredNode:
        self.configured_nodes = self.build_nodes()
        interface = self.build_node_interface()
        md = ConfiguredNode(
            name=self.cfg.name,
            node_type=NodeType.GRAPH,
            interface=interface,
            nodes=self.configured_nodes,
            original_cfg=self.cfg,
            # TODO: inputs and parameters at top-level? What about nested?
        )
        return md

    def build_node_interface(self) -> NodeInterface:
        if self.cfg.interface is None:
            if self.parent is None:
                return NodeInterface()
            else:
                raise NotImplementedError(
                    "Sub-graphs must declare an explicit interface"
                )
                # Not supported for now
                # return self.build_node_interface_from_child_interfaces()

        return self.build_node_interface_from_graph_interface()

    # Not supported for now
    # def build_node_interface_from_child_interfaces(self) -> NodeInterface:
    #     interface = NodeInterface()
    #     assert self.configured_nodes is not None
    #     for n in self.configured_nodes:
    #         interface = merge_interfaces(interface, n.interface)
    #     return interface

    def build_node_interface_from_graph_interface(self) -> NodeInterface:
        assert self.cfg.interface is not None
        inputs = OrderedDict()
        for i in self.cfg.interface.inputs:
            assert i.like is not None, "Must specify `like` for input"
            inpt = self.get_node_input_from_path(i.like)
            inputs[i.name] = inpt
        outputs = OrderedDict()
        for name, output_ref in self.cfg.interface.outputs.items():
            output = self.get_node_output_from_path(output_ref)
            outputs[name] = output
        parameters = OrderedDict()
        for name, value in self.cfg.interface.parameters.items():
            parameters[name] = Parameter(
                name=name, datatype=ParameterType("str"), default=value
            )
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
        return self.build_configured_node_from_node(
            graph_node_cfg, node, NodeType.PYTHON
        )

    def build_sql_configured_node(self, graph_node_cfg: GraphNodeCfg) -> ConfiguredNode:
        assert graph_node_cfg.sql is not None
        node = self.load_sql_node(graph_node_cfg.sql)
        return self.build_configured_node_from_node(graph_node_cfg, node, NodeType.SQL)

    def build_configured_node_from_node(
        self, graph_node_cfg: GraphNodeCfg, node: Node, node_type: NodeType
    ) -> ConfiguredNode:
        cfg_node = ConfiguredNode(
            name=node.name,
            node_type=node_type,
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
        relpath = graph_node_cfg.subgraph
        assert relpath.endswith(".yml") or relpath.endswith(".yaml")
        yaml_pth = self.directory / graph_node_cfg.subgraph
        dir_pth = yaml_pth.parent
        cfg = self.load_graph_cfg(str(yaml_pth))
        sub_builder = ConfiguredGraphBuilder(directory=dir_pth, cfg=cfg, parent=self,)
        return sub_builder.build_manifest_from_config()

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

    def load_sql_node(self, relpath: str) -> Node:
        return self.build_node_from_sql_file(relpath)

    def get_node_output_from_path(self, ref: str) -> IoBase:
        node_name, output_name = parse_node_output_path(ref)
        cfg_node = self.get_configured_node(node_name)
        if output_name is None:
            output = cfg_node.interface.get_default_output()
        else:
            output = cfg_node.interface.outputs[output_name]
        assert output is not None
        return output

    def get_node_input_from_path(self, ref: str) -> IoBase:
        node_name, input_name = parse_node_output_path(ref)
        cfg_node = self.get_configured_node(node_name)
        if input_name is None:
            inpt = cfg_node.interface.get_default_input()
        else:
            inpt = cfg_node.interface.inputs[input_name]
        assert inpt is not None
        return inpt

    def get_configured_node(self, name: str) -> ConfiguredNode:
        assert self.configured_nodes is not None
        for n in self.configured_nodes:
            if n.name == name:
                return n
        raise KeyError(name)

    def build_node_from_sql_file(self, relpath: str) -> Node:
        pth = Path(relpath)
        name = pth.name
        if name.endswith(".sql"):
            name = name[:-4]
        with self.set_current_path():
            tmpl = open(relpath).read()
            node = Node(
                name=name,
                node_callable=lambda ctx: ctx,
                language="sql",
                interface=parse_interface_from_sql(tmpl),
            )
        return node
