from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, fields, replace
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

from dags.utils.common import StringEnum

if TYPE_CHECKING:
    from dags.core.pipe import (
        PipeLike,
        Pipe,
    )
    from dags.core.module import DagsModule, DEFAULT_LOCAL_MODULE
    from dags.core.typing.object_type import ObjectTypeLike, ObjectType


class ComponentLibrary:
    pipes: Dict[str, Pipe]
    otypes: Dict[str, ObjectType]

    def __init__(self):
        self.pipes = {}
        self.otypes = {}

    def add_module(self, module: DagsModule):
        self.merge(module.library)

    def add_pipe(self, p: Pipe):
        self.pipes[p.key] = p

    def add_otype(self, otype: ObjectType):
        self.pipes[otype.key] = otype

    def get_pipe(self, pipe_like: Union[Pipe, str]) -> Pipe:
        from dags.core.pipe import Pipe

        if isinstance(pipe_like, Pipe):
            return pipe_like
        if not isinstance(pipe_like, str):
            raise TypeError(pipe_like)
        return self.pipes[pipe_like]

    def get_otype(self, otype_like: Union[ObjectType, str]) -> ObjectType:
        from dags.core.typing.object_type import ObjectType

        if isinstance(otype_like, ObjectType):
            return otype_like
        if not isinstance(otype_like, str):
            raise TypeError(otype_like)
        return self.otypes[otype_like]

    def all_pipes(self) -> List[Pipe]:
        return list(self.pipes.values())

    def all_otypes(self) -> List[ObjectType]:
        return list(self.otypes.values())

    def merge(self, other: ComponentLibrary):
        self.pipes.update(other.pipes)
        self.otypes.update(other.otypes)
