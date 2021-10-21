from __future__ import annotations

import inspect
import typing
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from functools import partial, wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from basis.configuration.graph import GraphCfg
from basis.configuration.node import GraphNodeCfg
from basis.node.interface import (
    DEFAULT_INPUT_NAME,
    DEFAULT_OUTPUT_NAME,
    IoBase,
    NodeInterface,
    OutputType,
    Parameter,
)

# from basis.utils.docstring import BasisParser, Docstring
from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame

if TYPE_CHECKING:
    from basis.execution.context import Context


class NodeException(Exception):
    pass


class InputExhaustedException(NodeException):
    pass


NodeCallable = Callable[["Context"], None]


def make_node_name(node: Union[NodeCallable, Node, str]) -> str:
    # TODO: something more principled / explicit?
    if isinstance(node, str):
        return node
    if hasattr(node, "name"):
        return node.name  # type: ignore
    if hasattr(node, "__name__"):
        return node.__name__
    if hasattr(node, "__class__"):
        return node.__class__.__name__
    raise Exception(f"Cannot make name for node-like {node}")


@dataclass(frozen=True)
class Node:
    name: str
    node_callable: Callable
    required_storage_classes: List[str] = field(default_factory=list)
    required_storage_engines: List[str] = field(default_factory=list)
    interface: NodeInterface = field(default_factory=NodeInterface)
    display_name: Optional[str] = None
    description: Optional[str] = None
    language: Optional[str] = "python"

    def __call__(self, ctx: Context) -> None:
        return self.node_callable(ctx)


# def parse_docstring(d: str) -> Docstring:
#     return BasisParser().parse(d)


def list_to_ordered_dict(lst: List) -> OrderedDict:
    return OrderedDict([(i.name, i) for i in lst or []])


def node_decorator(
    node_or_name: Union[str, NodeCallable, Node] = None,
    name: str = None,
    inputs: List[IoBase] = None,
    outputs: List[IoBase] = None,
    parameters: List[Parameter] = None,
    **kwargs,
) -> Callable:
    # if isinstance(node_or_name, Node):
    #     return node_or_name
    if isinstance(node_or_name, str) or node_or_name is None:
        return partial(
            node_decorator,
            name=name or node_or_name,
            inputs=inputs,
            outputs=outputs,
            parameters=parameters,
            **kwargs,
        )
    fn = node_or_name
    if name is None:
        name = make_node_name(fn)
    inputs_od = list_to_ordered_dict(inputs or [])
    outputs_od = list_to_ordered_dict(outputs or [])
    parameters_od = list_to_ordered_dict(parameters or [])
    return Node(
        name=name,
        node_callable=fn,
        interface=NodeInterface(
            inputs=inputs_od, outputs=outputs_od, parameters=parameters_od,
        ),
        **kwargs,
    )


def parse_node_output_path(pth: str) -> Tuple[str, Optional[str]]:
    node_name = pth
    output_name = None
    if "." in pth:
        node_name, output_name = pth.split(".")
    return node_name, output_name


# Decorator API
node = node_decorator
