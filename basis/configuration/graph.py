from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List

import pydantic
from pydantic import constr

from basis.configuration.base import FrozenPydanticBase


@dataclass(frozen=True)
class PortMappingCfg:
    src: str
    dst: str

    _regex = re.compile(r"^(\S+)\s*->\s*(\S+)$")

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(
            pattern=cls._regex.pattern,
            examples=["mytable -> input", "output -> mytable"],
        )

    @classmethod
    def validate(cls, v):
        if not (m := cls._regex.fullmatch(v)):
            raise ValueError("invalid alias format")
        return cls(*m.groups())


class NodeCfg(FrozenPydanticBase):
    # File node only
    node_file: str = None
    schedule: str = None
    inputs: List[PortMappingCfg] = None
    outputs: List[PortMappingCfg] = None
    parameters: Dict[str, Any] = None

    # Webhook only
    webhook: str = None

    # Available to both
    name: str = None
    id: constr(to_lower=True, regex=r"[a-zA-Z234567]{8}") = None
    description: str = None

    @pydantic.validator("webhook")
    def webhook_validator(cls, v, values):
        if v is not None:
            for k in (
                "node_file",
                "name",
                "schedule",
                "inputs",
                "outputs",
                "parameters",
            ):
                if values.get(k, None) is not None:
                    raise ValueError(
                        "Cannot specify both 'webhook' and 'k' in a single entry"
                    )
        return v


class ExposingCfg(FrozenPydanticBase):
    inputs: List[str] = None
    outputs: List[str] = None
    parameters: List[str] = None


class GraphDefinitionCfg(FrozenPydanticBase):
    name: str = None
    exposes: ExposingCfg = None
    description: str = None
    schedule: str = None
    nodes: List[NodeCfg] = None
