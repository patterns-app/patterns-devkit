from __future__ import annotations

from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.declarative.base import FrozenPydanticBase


class FlowCfg(FrozenPydanticBase):
    name: str
    namespace: str
    graph: GraphCfg

    @property
    def key(self) -> str:
        assert self.name and self.namespace
        return self.namespace + "." + self.name

