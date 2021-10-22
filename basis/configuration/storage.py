from __future__ import annotations

from typing import Iterator, Optional, Tuple, TypeVar, Union

from basis.configuration.base import FrozenPydanticBase


class StorageCfg(FrozenPydanticBase):
    name: str
    url: str
