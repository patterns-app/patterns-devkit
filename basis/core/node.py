from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import networkx as nx
from basis.core.component import ComponentLibrary, global_library
from basis.core.declarative.base import FrozenPydanticBase, PydanticBase
from basis.core.declarative.function import (
    DEFAULT_INPUT_NAME,
    DEFAULT_OUTPUT_NAME,
    FunctionCfg,
    FunctionInterfaceCfg,
)
from basis.core.declarative.node import NodeCfg, NodeOutputCfg
from basis.core.function import Function
from commonmodel import Schema
from dcp.utils.common import as_identifier, remove_dupes
from loguru import logger
from pydantic import validator
from pydantic.class_validators import root_validator

if TYPE_CHECKING:
    from basis.core.declarative.flow import FlowCfg
    from basis.core.declarative.interface import NodeInputCfg


NxNode = Tuple[str, Dict[str, Dict]]
NxAdjacencyList = List[NxNode]


def conform_inputs(inputs) -> Dict[str, str]:
    if isinstance(inputs, dict):
        return inputs
    if isinstance(inputs, str):
        inputs = [inputs]
    assert isinstance(inputs, list)
    assert len(inputs) == 1
    return {DEFAULT_INPUT_NAME: inputs[0]}


def conform_aliases(aliases) -> Dict[str, str]:
    if isinstance(aliases, dict):
        return aliases
    if isinstance(aliases, str):
        aliases = [aliases]
    assert isinstance(aliases, list)
    assert len(aliases) == 1
    return {DEFAULT_OUTPUT_NAME: aliases[0]}


def instantiate_node(cfg: NodeCfg, lib: ComponentLibrary) -> Node:
    d = cfg.dict()
    d["function"] = lib.get_function(cfg.function)
    d["original_cfg"] = cfg
    d["inputs"] = conform_inputs(cfg.inputs)
    d["aliases"] = conform_aliases(cfg.aliases)
    return Node(**d)


@dataclass
class Node:
    key: str  # = "default"
    function: Function
    #     function_cfg: Optional[FunctionCfg] = None
    params: Dict[str, Any] = None  # {}  # TODO: acceptable param types?
    stdin_key: Optional[str] = None
    stdout_key: Optional[str] = None
    stderr_key: Optional[str] = None
    inputs: Dict[str, str] = None  # {}
    outputs: Dict[str, NodeOutputCfg] = None  # {}
    aliases: Dict[str, str] = None
    # conform_to_schema: Optional[str] = None
    # schema_translations: Dict[str, Dict[str, str]] = {}
    schedule: Optional[str] = None
    runtime: Optional[str] = None
    storage: Optional[str] = None
    data_format: Optional[str] = None
    original_cfg: Optional[NodeCfg] = None

    def get_stdin_key(self) -> str:
        if self.stdin_key:
            return self.stdin_key
        if not self.nodes:
            return self.key
        raise Exception(
            f"Must specify 'stdin_key' when graph has multiple possible input nodes: {self}"
        )

    def get_stdout_key(self) -> str:
        if self.stdout_key:
            return self.stdout_key
        return self.key

    def get_inputs(self) -> Dict[str, str]:
        return self.inputs

    def get_all_schema_keys(self) -> List[str]:
        return self.get_interface().get_all_schema_keys()

    def get_interface(self) -> FunctionInterfaceCfg:
        return self.function.get_interface()
