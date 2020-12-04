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

from snapflow.utils.common import AttrDict, StringEnum

if TYPE_CHECKING:
    from snapflow.core.pipe import (
        PipeLike,
        Pipe,
    )
    from snapflow.core.module import SnapflowModule
    from snapflow.core.typing.schema import SchemaLike, Schema


class ComponentLibrary:
    pipes: Dict[str, Pipe]
    schemas: Dict[str, Schema]
    module_lookup_names: List[str]

    def __init__(self, module_lookup_keys: List[str] = None):
        from snapflow.core.module import DEFAULT_LOCAL_MODULE_NAME

        self.pipes = {}
        self.schemas = {}
        self.module_lookup_names = [DEFAULT_LOCAL_MODULE_NAME]
        if module_lookup_keys:
            for k in module_lookup_keys:
                self.add_module_name(k)

    def add_module_name(self, k: str):
        if k not in self.module_lookup_names:
            self.module_lookup_names.append(k)

    def add_module(self, module: SnapflowModule):
        self.merge(module.library)

    def add_pipe(self, p: Pipe):
        self.pipes[p.key] = p

    def add_schema(self, schema: Schema):
        self.schemas[schema.key] = schema

    def get_pipe(self, pipe_like: Union[Pipe, str], try_module_lookups=True) -> Pipe:
        from snapflow.core.pipe import Pipe

        if isinstance(pipe_like, Pipe):
            return pipe_like
        if not isinstance(pipe_like, str):
            raise TypeError(pipe_like)
        try:
            return self.pipes[pipe_like]
        except KeyError as e:
            if try_module_lookups:
                return self.module_name_lookup(self.pipes, pipe_like)
            raise e

    def get_schema(
        self, schema_like: Union[Schema, str], try_module_lookups=True
    ) -> Schema:
        from snapflow.core.typing.schema import Schema

        if isinstance(schema_like, Schema):
            return schema_like
        if not isinstance(schema_like, str):
            raise TypeError(schema_like)
        try:
            return self.schemas[schema_like]
        except KeyError as e:
            if try_module_lookups:
                return self.module_name_lookup(self.schemas, schema_like)
            raise e

    def module_name_lookup(self, d: Dict[str, Any], k: str) -> Any:
        if "." in k:
            raise KeyError(k)
        for m in self.module_lookup_names:
            try:
                return d[m + "." + k]
            except KeyError:
                pass
        raise KeyError(k)

    def all_pipes(self) -> List[Pipe]:
        return list(self.pipes.values())

    def all_schemas(self) -> List[Schema]:
        return list(self.schemas.values())

    def merge(self, other: ComponentLibrary):
        self.pipes.update(other.pipes)
        self.schemas.update(other.schemas)
        for k in other.module_lookup_names:
            self.add_module_name(k)

    def get_view(self, d: Dict) -> AttrDict:
        ad: AttrDict = AttrDict()
        for k, p in d.items():
            ad[k] = p
            ad[k.split(".")[-1]] = p  # TODO: module precedence
        return ad

    def get_pipes_view(self) -> AttrDict[str, Pipe]:
        return self.get_view(self.pipes)

    def get_schemas_view(self) -> AttrDict[str, Schema]:
        return self.get_view(self.schemas)
