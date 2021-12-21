from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List

import pydantic
from pydantic import constr
from pydantic.validators import str_validator
from basis.configuration.base import FrozenPydanticBase


class PortMappingCfg(str):
    """A map of port name to port name, represented by a string like 'src -> dst'"""

    _regex = re.compile(r"^(\S+) *-> *(\S+)$")

    @property
    def src(self):
        return self._regex.fullmatch(self).group(1)

    @property
    def dst(self):
        return self._regex.fullmatch(self).group(2)

    @classmethod
    def __get_validators__(cls):
        yield str_validator
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
        return cls(v)


class NodeCfg(FrozenPydanticBase):
    # File node only
    node_file: str = None
    schedule: str = None
    inputs: List[PortMappingCfg] = None
    outputs: List[PortMappingCfg] = None
    parameters: Dict[str, Any] = None

    # Webhook only
    webhook: str = None

    # Chart only
    chart_input: str = None

    # Available to all
    name: str = None
    id: constr(to_lower=True, regex=r"[a-zA-Z234567]{8}") = None
    description: str = None

    @pydantic.validator("webhook")
    def webhook_validator(cls, v, values):
        return cls._check_mutual_exclusion("webhook", v, values, ["node_file"])

    @pydantic.validator("chart_input")
    def chart_validator(cls, v, values):
        return cls._check_mutual_exclusion("chart_input", v, values)

    @classmethod
    def _check_mutual_exclusion(cls, field, v, values, extras=()):
        if v is None:
            return v
        fields = ["schedule", "inputs", "outputs", "parameters"] + list(extras)
        for k in fields:
            if values.get(k, None) is not None:
                raise ValueError(
                    f"Cannot specify both '{field}' and '{k}' in a single node"
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
