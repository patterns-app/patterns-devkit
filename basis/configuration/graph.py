from __future__ import annotations

import typing
from collections import OrderedDict
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

import re

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.node import GraphNodeCfg
from pydantic.fields import Field

re_port_path = re.compile(r"\s*(\w+)\[(\w+)\]\s*")
re_node_connection = re.compile(r"^({0})=>({0})$".format(re_port_path))


class BasisCfg(FrozenPydanticBase):
    version: str = None
    # TODO other global settings (retention policy, error handling, logging, etc)


class PortType(str, Enum):
    TABLE = "table"
    STREAM = "stream"
    PARAMETER = "parameter"


class PortPath:
    node: str
    port: str

    def __init__(self, path: str):
        m = re.match(f"^{re_port_path}$", path)
        if m is None:
            raise ValueError(path)
        self.node = m.group(1)
        self.port = m.group(2)

    def __str__(self) -> str:
        return f"{self.node}[{self.port}]"


class NodeConnection:
    input_path: PortPath
    output_path: PortPath

    def __init__(self, connection: str):
        m = re_node_connection.match(connection)
        if m is None:
            raise ValueError(connection)
        self.input_path = PortPath(m.group(1))
        self.output_path = PortPath(m.group(4))

    def __str__(self) -> str:
        return f"{self.input_path} => {self.output_path}"


class GraphPortCfg(FrozenPydanticBase):
    # TODO: this could be separate models
    name: str
    schema_like: Union[str, Schema] = Field(None, alias="schema")
    description: Optional[str] = None
    required: bool = True
    port_type: str = Field(str, alias="type")
    default_value: Any = None
    parameter_type: Optional[str] = None


class GraphNodeCfg(FrozenPydanticBase):
    name: str
    node_definition: str
    parameter_values: Dict[str, Any] = {}
    output_aliases: Dict[str, str]
    schedule: Optional[str] = None
    labels: Optional[List[str]] = None
    # TODO
    # default_output_configuration: NodeOutputCfg = NodeOutputCfg()
    # output_configurations: Dict[str, NodeOutputCfg] = {}


class GraphDefinitionCfg(FrozenPydanticBase):
    node_configurations: List[GraphNodeCfg] = []
    node_connections: List[NodeConnection] = []


class NodeType(str, Enum):
    PYTHON = "python"
    SQL = "sql"


class NodeDefinitionCfg(FrozenPydanticBase):
    node_type: NodeType
    node_script: str


class InterfaceBaseCfg(FrozenPydanticBase):
    input_ports: Optional[List[GraphPortCfg]] = None
    output_ports: Optional[List[GraphPortCfg]] = None
    parameter_ports: Optional[List[GraphPortCfg]] = None


class NodeCfg(InterfaceBaseCfg):
    node: Optional[NodeDefinitionCfg] = None


class GraphCfg(InterfaceBaseCfg):
    basis: BasisCfg = BasisCfg()
    graph: Optional[GraphDefinitionCfg] = None
