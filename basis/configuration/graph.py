from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List

from pydantic import constr

from basis.configuration.base import FrozenPydanticBase


@dataclass(frozen=True)
class PortMappingCfg:
    src: str
    dst: str

    _regex = re.compile(r'^(\S+)\s*->\s*(\S+)$')

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(
            pattern=cls._regex.pattern,
            examples=['mytable -> input', 'output -> mytable'],
        )

    @classmethod
    def validate(cls, v):
        if not (m := cls._regex.fullmatch(v)):
            raise ValueError('invalid alias format')
        return cls(*m.groups())


class NodeCfg(FrozenPydanticBase):
    node_file: str = None

    name: str = None
    id: constr(to_lower=True, regex=r'[a-zA-Z34567]{8}') = None
    description: str = None
    schedule: str = None
    inputs: List[PortMappingCfg] = None
    outputs: List[PortMappingCfg] = None
    parameters: Dict[str, Any] = None


class ExposingCfg(FrozenPydanticBase):
    inputs: List[str] = None
    outputs: List[str] = None
    parameters: List[str] = None


class GraphDefinitionCfg(FrozenPydanticBase):
    name: str = None
    exposes: ExposingCfg = None
    description: str = None
    schedule: str = None
    nodes: List[NodeCfg]
