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
from typing import Any, Iterator, Optional, Tuple, TypeVar, Union

from basis.configuration import graph
from basis.configuration.base import FrozenPydanticBase, load_yaml, update
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
class GraphManifestBuilder:
    directory: Path
    cfg: GraphCfg
    configured_nodes: Optional[list[ConfiguredNode]] = None
    parent: Optional[GraphManifestBuilder] = None

    def build_manifest_from_config(self) -> ConfiguredNode:
        self.configured_nodes = self.build_nodes()
        interface = self.build_node_interface()
        md = ConfiguredNode(
            name=self.cfg.name,
            node_type=NodeType.GRAPH,
            interface=interface,
            nodes=self.configured_nodes,
            original_cfg=self.cfg,
            # TODO: is it ever valid to have inputs and parameters on graph node? (nested handled in sub_builder below)
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

    def build_nodes(self) -> list[ConfiguredNode]:
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
        path_to_python_file_or_module = graph_node_cfg.python
        assert path_to_python_file_or_module is not None
        node = self.load_python_node(path_to_python_file_or_module)
        path_from_graph_root_to_node_file = None
        python_path_from_graph_root_to_node_object = None
        if path_to_python_file_or_module.endswith(".py"):
            path_from_graph_root_to_node_file = self.relative_to_graph_root(
                Path(path_to_python_file_or_module)
            )
        else:
            assert "/" not in path_to_python_file_or_module
            root_path = self.path_to_graph_root()
            root_module_path = str(root_path).replace("/", ".")
            # TODO: this may be node object or node module containing node object
            python_path_from_graph_root_to_node_object = (
                root_module_path + "." + path_to_python_file_or_module
            )
        cfg_node = ConfiguredNode(
            name=node.name,
            interface=node.interface,
            inputs=graph_node_cfg.inputs,
            parameters=graph_node_cfg.parameters,
            original_cfg=graph_node_cfg,
            node_type=NodeType.PYTHON,
            path_from_graph_root_to_node_file=str(path_from_graph_root_to_node_file),
            python_path_from_graph_root_to_node_object=python_path_from_graph_root_to_node_object,
        )
        return cfg_node

    def build_sql_configured_node(self, graph_node_cfg: GraphNodeCfg) -> ConfiguredNode:
        assert graph_node_cfg.sql is not None
        path_to_sql_file = graph_node_cfg.sql
        node = self.load_sql_node(path_to_sql_file)
        path_to_sql_file_from_graph_root = self.relative_to_graph_root(
            Path(path_to_sql_file)
        )
        cfg_node = ConfiguredNode(
            name=node.name,
            interface=node.interface,
            inputs=graph_node_cfg.inputs,
            parameters=graph_node_cfg.parameters,
            original_cfg=graph_node_cfg,
            node_type=NodeType.SQL,
            path_from_graph_root_to_node_file=str(path_to_sql_file_from_graph_root),
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
        # Build child graph
        sub_builder = GraphManifestBuilder(directory=dir_pth, cfg=cfg, parent=self,)
        cfg_node = sub_builder.build_manifest_from_config()
        # And finally set the inputs and parameters for inclusion in this parent graph
        return update(
            cfg_node, inputs=graph_node_cfg.inputs, parameters=graph_node_cfg.parameters
        )

    def relative_to_graph_root(self, pth: Path) -> Path:
        return self.path_to_graph_root() / pth

    def path_to_graph_root(self) -> Path:
        if self.parent is None:
            return Path(".")
        return self.directory.relative_to(self.parent.path_to_graph_root().resolve())

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
