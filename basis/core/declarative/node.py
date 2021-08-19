from __future__ import annotations

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
    FunctionCfg,
    FunctionInterfaceCfg,
)
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


def startswith_any(s: str, others: Iterable[str]) -> bool:
    for o in others:
        if not o:
            continue
        if s.startswith(o):
            return True
    return False


def ensure_enum_str(s: Union[str, Enum]) -> str:
    if isinstance(s, str):
        return s
    if isinstance(s, Enum):
        return s.value
    raise TypeError(s)


class ImproperlyConfigured(Exception):
    pass


# class DedupeBehavior(str, Enum):
#     NONE = "None"
#     LATEST_RECORD = "LatestRecord"
#     FIRST_NON_NULL_VALUES = "FirstNonNullValues"
#     LATEST_NON_NULL_VALUES = "LatestNonNullValues"


class NodeOutputCfg(FrozenPydanticBase):
    storage: Optional[str] = None
    data_format: Optional[str] = None
    # retention_policy: Optional[str] = None # TODO


class NodeCfg(FrozenPydanticBase):
    key: str  # = "default"
    # nodes: List[GraphCfg] = []
    function: Optional[str] = None
    function_cfg: Optional[FunctionCfg] = None
    params: Dict[str, Any] = {}  # TODO: acceptable param types?
    stdin_key: Optional[str] = None
    stdout_key: Optional[str] = None
    stderr_key: Optional[str] = None
    inputs: Dict[str, str] = {}
    outputs: Dict[str, NodeOutputCfg] = {}
    # aliases: Dict[str, str] = {}
    # conform_to_schema: Optional[str] = None
    # schema_translations: Dict[str, Dict[str, str]] = {}
    schedule: Optional[str] = None
    runtime: Optional[str] = None
    storage: Optional[str] = None
    data_format: Optional[str] = None

    def get_stdin_key(self) -> str:
        if self.stdin_key:
            return self.stdin_key
        return self.key

    def get_stdout_key(self) -> str:
        if self.stdout_key:
            return self.stdout_key
        return self.key

    # def get_inputs(self) -> Dict[str, str]:
    #     if self.input:
    #         return {"stdin": self.input}
    #     return self.inputs

    # def get_aliases(self) -> Dict[str, str]:
    #     if self.alias:
    #         return {"stdout": self.alias}
    #     return self.aliases

    # def get_schema_translations(self) -> Dict[str, Dict[str, str]]:
    #     if self.schema_translation:
    #         return {"stdin": self.schema_translation}
    #     return self.schema_translations

    def get_all_schema_keys(self) -> List[str]:
        return self.get_interface().get_all_schema_keys()

    def get_alias(self, node_key: Optional[str] = None) -> str:
        ident = None
        aliases = self.get_aliases()
        if aliases:
            ident = aliases.get(node_key or "stdout")
        if ident is None:
            ident = self.key
        return as_identifier(
            ident
        )  # TODO: this logic should be storage api specific! and then shared back?
