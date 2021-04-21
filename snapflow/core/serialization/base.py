from dataclasses import asdict, fields, is_dataclass
from typing import Any, Callable, Dict, Optional

from snapflow.core import operators
from snapflow.core.data_block import DataBlock
from snapflow.core.environment import (
    Environment,
    current_env,
    produce,
    run_graph,
    run_node,
)
from snapflow.core.function import DataFunction, Input, Output, Param
from snapflow.core.graph import DeclaredGraph, Graph, graph
from snapflow.core.module import SnapflowModule
from snapflow.core.node import DeclaredNode, Node, node
from snapflow.core.sql.sql_function import Sql, sql_function
from snapflow.core.streams import DataBlockStream, StreamBuilder

# TODO: use an existing lib for this:
#       - maybe the best: https://github.com/Fatal1ty/mashumaro


# def get_serializer(obj: Any) -> Optional[Callable[..., Dict]]:
#     if hasattr(obj, "_serialize"):
#         return obj._serialize
#     return None


# def _serialize(obj: Any, dict_factory: Callable[..., Dict]) -> Any:
#     """Adapted from dataclass.asdict"""
#     if is_dataclass(obj):
#         result = []
#         for f in fields(obj):
#             value = _serialize(getattr(obj, f.name), dict_factory)
#             result.append((f.name, value))
#         return dict_factory(result)
#     elif isinstance(obj, tuple) and hasattr(obj, "_fields"):
#         # obj is a namedtuple.  Recurse into it, but the returned
#         # object is another namedtuple of the same type.  This is
#         # similar to how other list- or tuple-derived classes are
#         # treated (see below), but we just need to create them
#         # differently because a namedtuple's __init__ needs to be
#         # called differently (see bpo-34363).

#         # I'm not using namedtuple's _asdict()
#         # method, because:
#         # - it does not recurse in to the namedtuple fields and
#         #   convert them to dicts (using dict_factory).
#         # - I don't actually want to return a dict here.  The main
#         #   use case here is json.dumps, and it handles converting
#         #   namedtuples to lists.  Admittedly we're losing some
#         #   information here when we produce a json list instead of a
#         #   dict.  Note that if we returned dicts here instead of
#         #   namedtuples, we could no longer call asdict() on a data
#         #   structure where a namedtuple was used as a dict key.

#         return type(obj)(*[_serialize(v, dict_factory) for v in obj])
#     elif isinstance(obj, (list, tuple)):
#         # Assume we can create an object of this type by passing in a
#         # generator (which is not true for namedtuples, handled
#         # above).
#         return type(obj)(_serialize(v, dict_factory) for v in obj)
#     elif isinstance(obj, dict):
#         return type(obj)(
#             (_serialize(k, dict_factory), _serialize(v, dict_factory))
#             for k, v in obj.items()
#         )
#     else:
#         s = get_serializer(obj)
#         if s is None:
#             return copy.deepcopy(obj)
#         return s(obj)
