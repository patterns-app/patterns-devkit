from __future__ import annotations
from enum import Enum

from typing import Any, Dict, List, Optional, Union

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.storage import StorageCfg
from pydantic.class_validators import root_validator


class NodeType(str, Enum):
    PYTHON = "python"
    SQL = "sql"
    GRAPH = "graph"


class NodeOutputCfg(FrozenPydanticBase):
    name: Optional[str] = None
    storage: Optional[Union[str, StorageCfg]] = None
    data_format: Optional[str] = None
    retention_policy: Optional[str] = None  # TODO


class GraphNodeCfg(FrozenPydanticBase):
    python: Optional[str] = None
    sql: Optional[str] = None
    subgraph: Optional[str] = None
    # TODO:
    # import: Optional[str] = None
    # reference: Optional[str] = None
    name: Optional[str] = None  # Overrides
    parameters: Dict[str, Any] = {}
    inputs: Union[List[str], Dict[str, str], str] = {}
    # node_resources: NodeResourcesCfg = NodeResourcesCfg() # TODO
    default_output_configuration: NodeOutputCfg = NodeOutputCfg()
    output_configurations: Dict[str, NodeOutputCfg] = {}
    schedule: Optional[str] = None

    @root_validator
    def check_node_type(cls, values: Dict) -> Dict:
        python = values.get("python")
        sql = values.get("sql")
        subgraph = values.get("subgraph")
        assert (
            len([s for s in [python, sql, subgraph] if s is not None]) == 1
        ), "Must define one and only one of python, sql, or subgraph"
        return values

    @property
    def node_type(self) -> NodeType:
        if self.python is not None:
            return NodeType.PYTHON
        if self.sql is not None:
            return NodeType.SQL
        if self.subgraph is not None:
            return NodeType.GRAPH
        raise ValueError("No node type")
