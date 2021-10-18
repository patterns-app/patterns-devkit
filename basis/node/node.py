from __future__ import annotations

import inspect
import typing
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from basis.core.block import Block
from basis.node.interface import (
    DEFAULT_INPUT_NAME,
    DEFAULT_OUTPUT_NAME,
    BlockType,
    IoBase,
    NodeInterface,
    Parameter,
)
from basis.utils.docstring import BasisParser, Docstring
from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame

if TYPE_CHECKING:
    from basis import Context
    from basis import Environment


class NodeException(Exception):
    pass


class InputExhaustedException(NodeException):
    pass


NodeCallable = Callable[[Context], None]

DataInterfaceType = Union[
    DataFrame,
    Records,
    Block,
]  # TODO: also input...?   Isn't this duplicated with the Interface list AND with DataFormats?


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


@dataclass
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


def parse_docstring(d: str) -> Docstring:
    return BasisParser().parse(d)


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
            inputs=inputs_od,
            outputs=outputs_od,
            parameters=parameters_od,
        ),
        **kwargs,
    )


def wrap_simple_node(node_callable: Callable) -> Callable:
    @wraps(node_callable)
    def node_wrapper(ctx: Context):
        if not ctx.inputs:
            # Generative node, just emit output
            result = node_callable()
            ctx.emit(result)
            return
        input_names = list(ctx.inputs)
        inputs = list(ctx.inputs.values())
        first_input = inputs[0]
        other_inputs = inputs[1:]
        for i in other_inputs:
            # Other inputs must be refs
            assert not isinstance(i, list)
        other_args = [ctx._as_managed_block(i) for i in other_inputs]
        for block in first_input:
            mb = ctx._as_managed_block(block)
            args = [mb]
            args.extend(other_args)
            try:
                result = node_callable(*args)
                ctx.emit(result)
                ctx.consume(input_names[0], mb)
            except Exception as e:
                ctx.emit_error(block, str(e))  # TODO: node traceback

    return node_wrapper


# Decorator API
node = node_decorator
