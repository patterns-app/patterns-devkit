from __future__ import annotations

import inspect
import typing
from collections import OrderedDict
from functools import partial, wraps
from typing import Callable, Optional, Union

from basis.execution.context import Context
from basis.node.interface import (
    DEFAULT_OUTPUT_NAME,
    DEFAULT_RECORD_OUTPUT,
    DEFAULT_TABLE_OUTPUT,
    IoBase,
    NodeInterface,
    Parameter,
    RecordStream,
    Table,
)
from basis.node.node import Node, NodeCallable, list_to_ordered_dict, make_node_name


def create_node_callable_from_simple_node(
    simple_node_callable: Callable, mode: str = "streaming"
) -> Callable:
    @wraps(simple_node_callable)
    def simple_node_wrapper(ctx: Context):
        interface = ctx.get_node_interface()
        if mode == "streaming":
            assert interface.is_streaming
        else:
            assert interface.is_table
        # First input may be stream or table
        input_names = list(interface.inputs)
        first_input_name = input_names[0]
        # Subsequent inputs must be tables, if any at all
        other_inputs = [ctx.get_table(n) for n in input_names[1:]]
        if mode == "streaming":
            # TODO: new streaming watcher
            for record in ctx.get_records(first_input_name):
                try:
                    result = simple_node_callable(record, *other_inputs)
                    ctx.append_record(first_input_name, result)
                    ctx.checkpoint(first_input_name)
                except Exception as e:
                    ctx.append_error(record, str(e))  # TODO: node traceback
                    # TODO: keep processing or bail, make this configurable
        else:
            try:
                result = simple_node_callable(
                    ctx.get_table(first_input_name), *other_inputs
                )
                ctx.store_as_table(first_input_name, result)
            except Exception as e:
                # TODO: if whole table fails, what to do?
                ctx.append_error(first_input_name, str(e))  # TODO: node traceback

    return simple_node_wrapper


def simple_node_decorator(
    node_or_name: str | NodeCallable | Node = None,
    name: str = None,
    inputs: list[IoBase] = None,
    outputs: list[IoBase] = None,
    parameters: list[Parameter] = None,
    mode: str = "streaming",
    **kwargs,
) -> Node | Callable:
    if isinstance(node_or_name, str) or node_or_name is None:
        return partial(
            simple_node_decorator,
            name=name or node_or_name,
            inputs=inputs,
            outputs=outputs,
            parameters=parameters,
            mode=mode,
            **kwargs,
        )
    node_callable = node_or_name
    if name is None:
        name = make_node_name(node_callable)
    inputs_od = list_to_ordered_dict(inputs or [])
    outputs_od = list_to_ordered_dict(outputs or [])
    parameters_od = list_to_ordered_dict(parameters or [])
    interface = node_interface_from_callable(node_callable, mode=mode)
    wrapped_fn = create_node_callable_from_simple_node(node_callable, mode=mode)
    # TODO:
    # doc = parse_docstring(node_callable.__doc__)
    return Node(
        name=name,
        node_callable=wrapped_fn,
        interface=NodeInterface(
            inputs=inputs_od or interface.inputs,
            outputs=outputs_od or interface.outputs,
            parameters=parameters_od or interface.parameters,
        ),
        # description=doc.short_description,
        **kwargs,
    )


def node_interface_from_callable(
    node: NodeCallable, mode: str = "streaming"
) -> NodeInterface:
    if hasattr(node, "get_interface"):
        return node.get_interface()
    signature = inspect.signature(node)
    outputs = OrderedDict()
    if mode == "streaming":
        outputs[DEFAULT_OUTPUT_NAME] = DEFAULT_RECORD_OUTPUT
    else:
        outputs[DEFAULT_OUTPUT_NAME] = DEFAULT_TABLE_OUTPUT
    inputs = OrderedDict()
    first = True
    for name, param in signature.parameters.items():
        optional = param.default is None
        if first:
            if mode == "streaming":
                # First input is stream
                inputs[name] = RecordStream(name=name)
            elif mode == "table":
                # First input is table
                inputs[name] = Table(name=name)
            else:
                raise ValueError(mode)
            first = False
        else:
            # Additional inputs must be table
            inputs[name] = Table(name=name, required=not optional)
    return NodeInterface(
        inputs=inputs,
        outputs=outputs,
    )


simple_streaming_node = partial(simple_node_decorator, mode="streaming")
simple_table_node = partial(simple_node_decorator, mode="table")
