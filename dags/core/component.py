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

from dags.utils.common import AttrDict, StringEnum

if TYPE_CHECKING:
    from dags.core.pipe import (
        PipeLike,
        Pipe,
    )
    from dags.core.module import DagsModule
    from dags.core.typing.object_type import ObjectTypeLike, ObjectType


class ComponentLibrary:
    pipes: Dict[str, Pipe]
    otypes: Dict[str, ObjectType]
    module_lookup_keys = List["DagsModule"]

    def __init__(self, module_lookup_keys: List[str] = None):
        from dags.core.module import DEFAULT_LOCAL_MODULE_KEY

        self.pipes = {}
        self.otypes = {}
        self.module_lookup_keys = [DEFAULT_LOCAL_MODULE_KEY]
        if module_lookup_keys:
            for k in module_lookup_keys:
                self.add_module_key(k)

    def add_module_key(self, k: str):
        if k not in self.module_lookup_keys:
            self.module_lookup_keys.append(k)

    def add_module(self, module: DagsModule):
        self.merge(module.library)

    def add_pipe(self, p: Pipe):
        self.pipes[p.key] = p

    def add_otype(self, otype: ObjectType):
        self.otypes[otype.key] = otype

    def get_pipe(self, pipe_like: Union[Pipe, str], try_module_lookups=True) -> Pipe:
        from dags.core.pipe import Pipe

        if isinstance(pipe_like, Pipe):
            return pipe_like
        if not isinstance(pipe_like, str):
            raise TypeError(pipe_like)
        try:
            return self.pipes[pipe_like]
        except KeyError as e:
            if try_module_lookups:
                return self.module_key_lookup(self.pipes, pipe_like)
            raise e

    def get_otype(
        self, otype_like: Union[ObjectType, str], try_module_lookups=True
    ) -> ObjectType:
        from dags.core.typing.object_type import ObjectType

        if isinstance(otype_like, ObjectType):
            return otype_like
        if not isinstance(otype_like, str):
            raise TypeError(otype_like)
        try:
            return self.otypes[otype_like]
        except KeyError as e:
            if try_module_lookups:
                return self.module_key_lookup(self.otypes, otype_like)
            raise e

    def module_key_lookup(self, d: Dict[str, Any], k: str) -> Any:
        if "." in k:
            raise KeyError(k)
        for m in self.module_lookup_keys:
            try:
                return d[m + "." + k]
            except KeyError:
                pass
        raise KeyError(k)

    def all_pipes(self) -> List[Pipe]:
        return list(self.pipes.values())

    def all_otypes(self) -> List[ObjectType]:
        return list(self.otypes.values())

    def merge(self, other: ComponentLibrary):
        self.pipes.update(other.pipes)
        self.otypes.update(other.otypes)
        for k in other.module_lookup_keys:
            self.add_module_key(k)

    def get_view(self, d: Dict) -> AttrDict:
        ad = AttrDict()
        for k, p in d.items():
            ad[k] = p
            ad[k.split(".")[-1]] = p
        return ad

    def get_pipes_view(self) -> AttrDict[str, Pipe]:
        return self.get_view(self.pipes)

    def get_otypes_view(self) -> AttrDict[str, ObjectType]:
        return self.get_view(self.otypes)
