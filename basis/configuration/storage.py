from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration.base import FrozenPydanticBase


class StorageCfg(FrozenPydanticBase):
    name: str
    url: str

