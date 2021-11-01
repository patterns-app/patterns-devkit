from __future__ import annotations

import re
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.path import NodeConnection
from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator
from commonmodel import Schema


class BasisCfg(FrozenPydanticBase):
    version: str = None
    # TODO other global settings (retention policy, error handling, logging, etc)


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
    node_definition: str
    name: Optional[str] = None
    parameter_values: Dict[str, Any] = {}
    output_aliases: Dict[str, str] = {}
    schedule: Optional[str] = None
    labels: Optional[List[str]] = None
    # TODO
    # default_output_configuration: NodeOutputCfg = NodeOutputCfg()
    # output_configurations: Dict[str, NodeOutputCfg] = {}


class GraphDefinitionCfg(FrozenPydanticBase):
    node_configurations: List[GraphNodeCfg] = []
    node_connections: List[NodeConnection] = []

    @validator("node_connections", pre=True)
    def check_node_connections(cls, connections: Any) -> list[NodeConnection]:
        conns = []
        for conn in connections:
            if isinstance(conn, str):
                conns.append(NodeConnection.from_str(conn))
            elif isinstance(conn, dict):
                conns.append(NodeConnection(**conn))
            else:
                raise TypeError(conn)
        return conns


class ScriptType(str, Enum):
    PYTHON = "python"
    SQL = "sql"


class ScriptCfg(FrozenPydanticBase):
    script_type: ScriptType
    script_definition: str


class InterfaceCfg(FrozenPydanticBase):
    input_ports: Optional[List[GraphPortCfg]] = None
    output_ports: Optional[List[GraphPortCfg]] = None
    parameter_ports: Optional[List[GraphPortCfg]] = None


class NodeDefinitionCfg(InterfaceCfg):
    basis: Optional[BasisCfg] = None
    description: Optional[str] = None
    graph: Optional[GraphDefinitionCfg] = None
    script: Optional[ScriptCfg] = None
    name: Optional[str] = None
