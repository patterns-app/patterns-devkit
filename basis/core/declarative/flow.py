from __future__ import annotations

from typing import List

from basis.core.declarative.base import FrozenPydanticBase


class FlowCfg(FrozenPydanticBase):
    name: str
    namespace: str
    nodes: List[NodeCfg]

    @property
    def key(self) -> str:
        assert self.name and self.namespace
        return self.namespace + "." + self.name
