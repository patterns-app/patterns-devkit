from dataclasses import dataclass, asdict
from typing import Any, Union, List, Dict

from commonmodel import Schema

from basis.configuration.path import GraphEdge, NodeId, PortId
from basis.graph.configured_node import (
    ParameterDefinition,
    ParameterType,
    OutputDefinition,
    PortType,
    InputDefinition,
    ConfiguredNode,
    NodeType,
    NodeInterface,
)


class _IgnoreType:
    def __eq__(self, other):
        return isinstance(other, _IgnoreType)

    def __repr__(self):
        return "IGNORE"


IGNORE = _IgnoreType()


@dataclass
class NodeAssertion:
    name: Union[str, _IgnoreType]
    node_type: Union[NodeType, _IgnoreType]
    id: Union[NodeId, _IgnoreType]
    parent_node_id: Union[str, None, _IgnoreType]
    interface: Union[NodeInterface, _IgnoreType]
    description: Union[str, _IgnoreType]
    file_path_to_node_script_relative_to_root: Union[str, _IgnoreType]
    parameter_values: Union[Dict[str, Any], _IgnoreType]
    schedule: Union[str, _IgnoreType]
    local_edges: Union[List[str], _IgnoreType]
    resolved_edges: Union[List[str], _IgnoreType]


def _calculate_graph(nodes: List[ConfiguredNode]):
    nodes_by_id = {n.id: n for n in nodes}
    ids_by_path = {}

    for node in nodes:
        parts = [node.name]
        parent = node.parent_node_id
        while parent:
            n = nodes_by_id[parent]
            parts.append(n.name)
            parent = n.parent_node_id
        ids_by_path[".".join(reversed(parts))] = node.id

    return ids_by_path


def _assert_node(node: ConfiguredNode, assertion: NodeAssertion, ids_by_path: dict, paths_by_id: dict):
    for k, v in asdict(assertion).items():
        actual = getattr(node, k)
        if k == "id":
            assert actual, "all ids must be defined"
        if v == IGNORE:
            continue

        if k == 'resolved_edges':
            continue  # todo: check these
        if k in ("local_edges", "resolved_edges"):
            v = [_ge(s, ids_by_path) for s in v]
            # compare ignoring order
            assert len(v) == len(actual)
            v = set(v)
            actual = set(actual)
        elif k == "parent_node_id" and v is not None:
            v = ids_by_path[v]

        assert actual == v, f"{k}: {_tostr(actual, paths_by_id)} != {_tostr(v, paths_by_id)}"


def _tostr(it, paths_by_id: dict) -> str:
    if isinstance(it, (list, set)):
        return str([_tostr(i, paths_by_id) for i in it])
    if isinstance(it, PortId):
        return f'{paths_by_id[it.node_id]}:{it.port}'
    if isinstance(it, GraphEdge):
        return f'{_tostr(it.input_path, paths_by_id)} -> {_tostr(it.output_path, paths_by_id)}'
    return repr(it)


def assert_nodes(
    nodes: List[ConfiguredNode], *expected: NodeAssertion, assert_length: bool = True
):
    assert len(nodes) == len({node.id for node in nodes}), "all ids must be unique"
    if assert_length:
        assert len(nodes) == len(expected)

    nodes_by_name = {node.name: node for node in nodes}
    ids_by_path = _calculate_graph(nodes)
    paths_by_id = {v: k for (k, v) in ids_by_path.items()}
    for assertion in expected:
        assert assertion.name in nodes_by_name, f"No node named {assertion.name}"
        _assert_node(nodes_by_name[assertion.name], assertion, ids_by_path, paths_by_id)


# noinspection PyDefaultArgument
def n(
    name: str,
    node_type: Union[NodeType, _IgnoreType] = NodeType.Node,
    id: Union[str, _IgnoreType] = IGNORE,
    parent: Union[str, None, _IgnoreType] = None,
    interface: Union[
        List[Union[InputDefinition, OutputDefinition, ParameterDefinition]], _IgnoreType
    ] = IGNORE,
    description: Union[str, None, _IgnoreType] = None,
    file_path: Union[str, _IgnoreType] = IGNORE,
    parameter_values: Union[Dict[str, Any], _IgnoreType] = {},
    schedule: Union[str, None, _IgnoreType] = None,
    local_edges: Union[List[str], _IgnoreType] = IGNORE,
    resolved_edges: Union[List[str], None, _IgnoreType] = None,
) -> NodeAssertion:
    return NodeAssertion(
        name=name,
        node_type=node_type,
        id=IGNORE if id == IGNORE else NodeId(id.lower()),
        parent_node_id=parent,
        interface=interface
        if interface == IGNORE
        else NodeInterface(
            inputs=[i for i in interface if isinstance(i, InputDefinition)],
            outputs=[i for i in interface if isinstance(i, OutputDefinition)],
            parameters=[i for i in interface if isinstance(i, ParameterDefinition)],
        ),
        description=description,
        file_path_to_node_script_relative_to_root=file_path,
        parameter_values=parameter_values,
        schedule=schedule,
        local_edges=local_edges,
        resolved_edges=local_edges if resolved_edges is None else resolved_edges
        if resolved_edges is None
        else resolved_edges,
    )


def p(
    name: str, parameter_type: str = None, description: str = None, default: Any = None,
) -> ParameterDefinition:
    return ParameterDefinition(
        name=name,
        parameter_type=ParameterType(parameter_type) if parameter_type else None,
        description=description,
        default=default,
    )


def ostream(
    name: str, description: str = None, schema: Union[str, Schema] = None,
) -> OutputDefinition:
    return OutputDefinition(
        port_type=PortType.Stream,
        name=name,
        description=description,
        schema_or_name=schema,
    )


def istream(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
    required: bool = True,
) -> InputDefinition:
    return InputDefinition(
        port_type=PortType.Stream,
        name=name,
        description=description,
        schema_or_name=schema,
        required=required,
    )


def itable(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
    required: bool = True,
) -> InputDefinition:
    return InputDefinition(
        port_type=PortType.Table,
        name=name,
        description=description,
        schema_or_name=schema,
        required=required,
    )


def otable(
    name: str, description: str = None, schema: Union[str, Schema] = None,
) -> OutputDefinition:
    return OutputDefinition(
        port_type=PortType.Table,
        name=name,
        description=description,
        schema_or_name=schema,
    )


def _ge(s: str, ids_by_path: dict) -> GraphEdge:
    l, r = s.split(" -> ")
    ln, lp = l.split(":")
    rn, rp = r.split(":")
    return GraphEdge(
        input_path=PortId(node_id=ids_by_path[ln], port=lp),
        output_path=PortId(node_id=ids_by_path[rn], port=rp),
    )
