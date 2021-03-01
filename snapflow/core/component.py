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
    from snapflow.core.snap import (
        SnapLike,
        _Snap,
    )
    from snapflow.core.module import SnapflowModule
    from snapflow.schema.base import SchemaLike, Schema


class ComponentLibrary:
    snaps: Dict[str, _Snap]
    schemas: Dict[str, Schema]
    module_lookup_names: List[str]

    def __init__(self, module_lookup_keys: List[str] = None):
        from snapflow.core.module import DEFAULT_LOCAL_MODULE_NAME

        self.snaps = {}
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

    def add_snap(self, p: _Snap):
        self.snaps[p.key] = p

    def add_schema(self, schema: Schema):
        self.schemas[schema.key] = schema

    def get_snap(self, snap_like: Union[_Snap, str], try_module_lookups=True) -> _Snap:
        from snapflow.core.snap import _Snap

        if isinstance(snap_like, _Snap):
            return snap_like
        if not isinstance(snap_like, str):
            raise TypeError(snap_like)
        try:
            return self.snaps[snap_like]
        except KeyError as e:
            if try_module_lookups:
                return self.module_name_lookup(self.snaps, snap_like)
            raise e

    def get_schema(
        self, schema_like: Union[Schema, str], try_module_lookups=True
    ) -> Schema:
        from snapflow.schema.base import Schema

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
        raise KeyError(f"`{k}` not found in modules {self.module_lookup_names}")

    def all_snaps(self) -> List[_Snap]:
        return list(self.snaps.values())

    def all_schemas(self) -> List[Schema]:
        return list(self.schemas.values())

    def merge(self, other: ComponentLibrary):
        self.snaps.update(other.snaps)
        self.schemas.update(other.schemas)
        for k in other.module_lookup_names:
            self.add_module_name(k)

    def get_view(self, d: Dict) -> AttrDict:
        ad: AttrDict = AttrDict()
        for k, p in d.items():
            ad[k] = p
            ad[k.split(".")[-1]] = p  # TODO: module precedence
        return ad

    def get_snaps_view(self) -> AttrDict[str, _Snap]:
        return self.get_view(self.snaps)

    def get_schemas_view(self) -> AttrDict[str, Schema]:
        return self.get_view(self.schemas)
