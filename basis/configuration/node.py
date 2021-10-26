from __future__ import annotations

from enum import Enum
import re
from typing import Any, Dict, List, Optional, Union

from pydantic.utils import path_type

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.storage import StorageCfg
from pydantic.class_validators import root_validator, validator


NODE_PATH_GRAPH_SEPARATOR = "."
NODE_PATH_OUTPUT_SEPARATOR = "@"
re_valid_node_name = re.compile(r"^[a-z_][a-z0-9_]$")
re_valid_node_path = re.compile(r"^[a-z_][a-z0-9_.]+(@[a-z]\w+)?$")


def is_valid_node_path(path: str) -> bool:
    return re_valid_node_path.match(path) is not None


"mygraph.subgraph.node1@output"
"basis://subgraph.node/inputs/input1"
"basis://mygraph.subgraph.node/outputs/output1.stream"
"basis://mygraph.subgraph.node/outputs/output2.table?environment=production"


class NodePath:
    path_to_node: list[str]
    io_name: str | None

    def __init__(self, path: str):
        assert is_valid_node_path(path), f"Invalid node path `{path}`"
        parts = path.split(NODE_PATH_GRAPH_SEPARATOR)
        self.path_to_node = parts[:-1]
        self.io_name = None
        output = parts[-1]
        if NODE_PATH_OUTPUT_SEPARATOR in output:
            node, output = output.split(NODE_PATH_OUTPUT_SEPARATOR)
            if node:
                self.path_to_node.append(node)
            self.io_name = output
        else:
            self.path_to_node.append(output)

    def path_to_node_str(self) -> str:
        pth = NODE_PATH_GRAPH_SEPARATOR.join(self.path_to_node)
        return pth

    def __str__(self) -> str:
        pth = self.path_to_node_str()
        if self.io_name:
            pth += NODE_PATH_OUTPUT_SEPARATOR + self.io_name
        return pth

    def __repr__(self) -> str:
        return str(self)


class NodeType(str, Enum):
    PYTHON = "python"
    SQL = "sql"
    GRAPH = "graph"
    COMPONENT = "component"


class NodeOutputCfg(FrozenPydanticBase):
    name: Optional[str] = None
    storage: Optional[Union[StorageCfg, str]] = None
    data_format: Optional[str] = None
    retention_policy: Optional[str] = None  # TODO


class GraphNodeCfg(FrozenPydanticBase):
    python: Optional[str] = None
    sql: Optional[str] = None
    subgraph: Optional[str] = None
    component: Optional[str] = None
    # TODO:
    # reference: Optional[str] = None
    name: Optional[str] = None  # Overrides
    parameters: Dict[str, Any] = {}
    inputs: Union[List[str], Dict[str, str], str] = {}
    # node_resources: NodeResourcesCfg = NodeResourcesCfg() # TODO
    outputs: Dict[str, str]  # TODO
    default_output_configuration: NodeOutputCfg = NodeOutputCfg()
    output_configurations: Dict[str, NodeOutputCfg] = {}
    schedule: Optional[str] = None

    @root_validator
    def check_node_type(cls, values: dict) -> dict:
        node_types = [n.value for n in NodeType]
        vals = [values.get(nt) for nt in node_types]
        assert (
            len([s for s in vals if s is not None]) == 1
        ), f"Must define one and only one node type {node_types}"
        return values

    @validator("inputs")
    def check_inputs(
        self, value: list[str] | dict[str, str] | str
    ) -> list[str] | dict[str, str] | str:
        node_paths = []
        if isinstance(value, list):
            node_paths = value
        elif isinstance(value, dict):
            node_paths = value.values()
        else:
            assert isinstance(value, str)
            node_paths = [value]
        for input_path in node_paths:
            assert is_valid_node_path(input_path), (
                f"Invalid input: `{input_path}` is not a valid node path"
                "(Example valid paths: `path.to.node@output_name` or `node` or `@output_name`)"
            )
        return value

    @property
    def node_type(self) -> NodeType:
        if self.python is not None:
            return NodeType.PYTHON
        if self.sql is not None:
            return NodeType.SQL
        if self.subgraph is not None:
            return NodeType.GRAPH
        raise ValueError("No node type")
