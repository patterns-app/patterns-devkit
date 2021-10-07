from __future__ import annotations

from typing import Any, Dict, List

from basis.configuration.base import FrozenPydanticBase
from basis.configuration.node import NodeCfg


class AppCfg(FrozenPydanticBase):
    name: str
    app_params: Dict[str, Any] = {}
    nodes: List[NodeCfg] = []

