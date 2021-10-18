import inspect
import typing
from collections import OrderedDict
from functools import partial, wraps
from typing import Callable, List, Optional, Union

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
        inputs = list(ctx.inputs.values())
        if mode == "streaming":
            assert ctx.interface.is_streaming
        else:
            assert ctx.interface.is_table
        input_blocks = [ctx._as_managed_block(i) for i in inputs]
        first_input = input_blocks[0]
        other_inputs = input_blocks[1:]
        if mode == "streaming":
            # TODO: new streaming watcher
            for record in first_input.as_records():
                try:
                    result = simple_node_callable(record, *other_inputs)
                    ctx.emit_record(result)
                    # TODO
                    # ctx.consume_up_to(input_names[0], mb)
                except Exception as e:
                    ctx.emit_error(record, str(e))  # TODO: node traceback
                    # TODO: keep processing or bail, make this configurable
        else:
            try:
                result = simple_node_callable(first_input, *other_inputs)
                ctx.emit_table(result)
            except Exception as e:
                ctx.emit_error(first_input, str(e))  # TODO: node traceback

    return simple_node_wrapper


def simple_node_decorator(
    node_or_name: Union[str, NodeCallable, Node] = None,
    name: str = None,
    inputs: List[IoBase] = None,
    outputs: List[IoBase] = None,
    parameters: List[Parameter] = None,
    mode: str = "streaming",
    **kwargs,
) -> Union[Node, Callable]:
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
